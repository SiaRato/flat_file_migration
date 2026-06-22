"""Transfer orchestrator (Functional).

Coordinates the folder-by-folder migration lifecycle:
    1. Migrate files concurrently (SFTP → S3 streaming) using Map/Reduce
    2. Track folder progress in local SQLite
"""

import logging
import posixpath
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List, Optional, Tuple

from migrator.config import AppConfig, SFTPConfig
from migrator.types import S3Context, SFTPContext, ManifestContext
from migrator.s3_client import S3AuthError, S3UploadError, upload_stream, delete_object
from migrator.sftp_client import (
    FileInfo,
    SFTPConnectionError,
    create_sftp_context,
    close_sftp_context,
    iter_files,
    open_file
)
from migrator.manifest import (
    STATUS_COMPLETED,
    STATUS_FAILED,
    STATUS_IN_PROGRESS,
    FILE_STATUS_FAILED,
    FILE_STATUS_SUCCESS,
    load_or_create_manifest,
    needs_transfer,
    mark_seen,
    record_transfer,
    reset_seen_flags,
    get_unseen_success_files,
    remove_file,
    set_status,
    all_successful,
    commit_manifest,
    get_failed_count
)

logger = logging.getLogger("migrator.transfer")


class FatalMigrationError(Exception):
    """Unrecoverable error — the program should halt."""
    pass


@dataclass
class MigrationStats:
    """Accumulated statistics across all folders."""
    total_files: int = 0
    transferred: int = 0
    skipped: int = 0
    failed: int = 0
    bytes_transferred: int = 0
    folders_completed: int = 0
    folders_failed: int = 0
    start_time: float = 0.0

    def summary(self) -> str:
        elapsed = time.time() - self.start_time
        mb = self.bytes_transferred / (1024 * 1024)
        return (
            f"\n{'=' * 60}\n"
            f"  Migration Summary\n"
            f"{'=' * 60}\n"
            f"  Folders completed : {self.folders_completed}\n"
            f"  Folders failed    : {self.folders_failed}\n"
            f"  Total files       : {self.total_files}\n"
            f"  Transferred       : {self.transferred}\n"
            f"  Skipped (up-to-date): {self.skipped}\n"
            f"  Failed            : {self.failed}\n"
            f"  Data transferred  : {mb:,.1f} MB\n"
            f"  Duration          : {elapsed:,.1f}s\n"
            f"{'=' * 60}"
        )


@dataclass
class TransferResult:
    """Result of a single file transfer map operation."""
    file_info: FileInfo
    success: bool
    bytes_transferred: int
    error: Optional[Exception] = None


def _resolve_tracking_dir(base_dir: str) -> str:
    import os
    tracking_dir = os.path.join(base_dir, "tracking")
    os.makedirs(tracking_dir, exist_ok=True)
    return tracking_dir





def _process_folder(
    folder: str,
    target: str,
    config: AppConfig,
    s3_ctx: S3Context,
    tracking_dir: str,
    folders_path: str,
    multipart_threshold: int,
    dry_run: bool,
    continuous: bool = False,
    stats: Optional[MigrationStats] = None,
) -> None:
    if stats is None:
        stats = MigrationStats(start_time=time.time())

    manifest_ctx = load_or_create_manifest(tracking_dir, folder)

    set_status(manifest_ctx, STATUS_IN_PROGRESS)
    commit_manifest(manifest_ctx)

    reset_seen_flags(manifest_ctx)

    consecutive_failures = 0
    max_consecutive = config.options.consecutive_failure_threshold
    batch_size = 5000  # Number of files to pull into thread pool at a time

    try:
        sftp_ctx = create_sftp_context(config.sftp)
    except SFTPConnectionError as e:
        set_status(manifest_ctx, STATUS_FAILED)
        commit_manifest(manifest_ctx)
        raise FatalMigrationError(f"SFTP connection failed: {e}") from e

    file_generator = iter_files(sftp_ctx, folder)
    
    with ThreadPoolExecutor(max_workers=config.options.max_workers) as pool:
        while True:
            # Buffer a batch of files
            batch = []
            try:
                for _ in range(batch_size):
                    batch.append(next(file_generator))
            except StopIteration:
                pass
            except SFTPConnectionError as e:
                set_status(manifest_ctx, STATUS_FAILED)
                commit_manifest(manifest_ctx)
                close_sftp_context(sftp_ctx)
                raise FatalMigrationError(f"SFTP connection lost during listing: {e}") from e

            if not batch:
                break
            
            stats.total_files += len(batch)

            to_transfer = []
            for fi in batch:
                if needs_transfer(manifest_ctx, fi.path, fi.size, fi.mtime):
                    to_transfer.append(fi)
                else:
                    mark_seen(manifest_ctx, fi.path)
                    stats.skipped += 1

            if dry_run:
                if to_transfer:
                    logger.info("DRY RUN — would transfer %d files in this batch", len(to_transfer))
                    for fi in to_transfer:
                        mark_seen(manifest_ctx, fi.path) # still mark seen so it doesn't get deleted
                continue

            futures = []
            for fi in to_transfer:
                try:
                    relative_path = posixpath.relpath(fi.path, folder)
                except ValueError:
                    # Fallback if paths are disjoint
                    relative_path = fi.path.lstrip("/")
                
                s3_key = posixpath.join(target, relative_path)
                
                future = pool.submit(
                    _transfer_single_file_map,
                    sftp_config=config.sftp,
                    s3_ctx=s3_ctx,
                    file_info=fi,
                    s3_key=s3_key,
                    multipart_threshold=multipart_threshold,
                    multipart_chunk_mb=config.options.multipart_chunk_mb,
                    max_retries=config.options.max_retries,
                    retry_backoff_base=config.options.retry_backoff_base,
                )
                futures.append(future)

            for future in as_completed(futures):
                try:
                    result: TransferResult = future.result()
                    fi = result.file_info
                    
                    if result.success:
                        record_transfer(
                            manifest_ctx,
                            path=fi.path,
                            size=fi.size,
                            mtime=fi.mtime,
                            s3_key=fi.path.lstrip("/"),
                            status=FILE_STATUS_SUCCESS,
                        )
                        stats.transferred += 1
                        stats.bytes_transferred += result.bytes_transferred
                        consecutive_failures = 0
                        logger.info("✓ %s (%.2f MB)", fi.path, fi.size / (1024 * 1024))
                    else:
                        e = result.error
                        record_transfer(
                            manifest_ctx,
                            path=fi.path, size=fi.size, mtime=fi.mtime,
                            s3_key=fi.path.lstrip("/"), status=FILE_STATUS_FAILED,
                        )
                        
                        if isinstance(e, (S3AuthError, SFTPConnectionError)):
                            set_status(manifest_ctx, STATUS_FAILED)
                            commit_manifest(manifest_ctx)
                            for pending_future in futures:
                                pending_future.cancel()
                            close_sftp_context(sftp_ctx)
                            raise FatalMigrationError(f"Fatal error during transfer: {e}") from e
                            
                        stats.failed += 1
                        consecutive_failures += 1
                        logger.error("✗ %s — %s", fi.path, e)

                        if consecutive_failures >= max_consecutive:
                            set_status(manifest_ctx, STATUS_FAILED)
                            commit_manifest(manifest_ctx)
                            for pending_future in futures:
                                pending_future.cancel()
                            close_sftp_context(sftp_ctx)
                            raise FatalMigrationError(f"{consecutive_failures} consecutive file failures")
                            
                except Exception as critical_e:
                    logger.error("Critical error mapping future: %s", critical_e)
                    raise FatalMigrationError(f"Critical mapping error: {critical_e}") from critical_e

            commit_manifest(manifest_ctx)

    close_sftp_context(sftp_ctx)

    # Mirror sync deletions
    to_delete = get_unseen_success_files(manifest_ctx)
    if dry_run and to_delete:
        logger.info("DRY RUN — would delete %d missing files from S3", len(to_delete))
    elif to_delete:
        for path, s3_key in to_delete:
            try:
                delete_object(s3_ctx, s3_key)
                remove_file(manifest_ctx, path)
            except Exception as e:
                logger.error("Failed to delete orphaned object %s from S3: %s", s3_key, e)
    commit_manifest(manifest_ctx)

    if all_successful(manifest_ctx):
        set_status(manifest_ctx, STATUS_COMPLETED)
        commit_manifest(manifest_ctx)
            
        stats.folders_completed += 1
        logger.info("✓ Folder completed: %s", folder)
    else:
        set_status(manifest_ctx, STATUS_FAILED)
        commit_manifest(manifest_ctx)

        stats.folders_failed += 1
        logger.warning(
            "✗ Folder has failures: %s (%d failed files — will retry on next run)",
            folder,
            get_failed_count(manifest_ctx),
        )


def _transfer_single_file_map(
    sftp_config: SFTPConfig,
    s3_ctx: S3Context,
    file_info: FileInfo,
    s3_key: str,
    multipart_threshold: int,
    multipart_chunk_mb: int,
    max_retries: int,
    retry_backoff_base: int,
) -> TransferResult:
    """Map function for transferring a single file."""
    last_error: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        sftp_ctx = None
        try:
            sftp_ctx = create_sftp_context(sftp_config)
            file_obj = open_file(sftp_ctx, file_info.path)

            upload_stream(
                ctx=s3_ctx,
                file_obj=file_obj,
                s3_key=s3_key,
                size=file_info.size,
                multipart_threshold=multipart_threshold,
                multipart_chunk_mb=multipart_chunk_mb,
            )

            return TransferResult(
                file_info=file_info,
                success=True,
                bytes_transferred=file_info.size
            )

        except (S3AuthError, SFTPConnectionError) as e:
            # Wrap immediately on fatal errors
            return TransferResult(file_info=file_info, success=False, bytes_transferred=0, error=e)
        except Exception as e:
            last_error = e
            if attempt < max_retries:
                backoff = retry_backoff_base ** attempt
                logger.warning(
                    "Retry %d/%d for %s (backoff %ds): %s",
                    attempt, max_retries, file_info.path, backoff, e,
                )
                time.sleep(backoff)
            else:
                logger.error(
                    "All %d retries exhausted for %s: %s",
                    max_retries, file_info.path, e,
                )
        finally:
            if sftp_ctx:
                close_sftp_context(sftp_ctx)

    return TransferResult(
        file_info=file_info,
        success=False,
        bytes_transferred=0,
        error=last_error or Exception(f"Transfer failed for {file_info.path}")
    )
