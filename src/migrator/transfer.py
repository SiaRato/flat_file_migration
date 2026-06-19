"""Transfer orchestrator.

Coordinates the folder-by-folder migration lifecycle:
    1. Upload 'in_progress' tracking to S3
    2. Migrate files concurrently (SFTP → S3 streaming)
    3. Upload 'completed' or 'failed' tracking to S3
    4. Clean up local tracking + mark folder done
"""

import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List, Optional

from migrator.config import AppConfig, SFTPConfig, mark_folder_done
from migrator.manifest import (
    Manifest,
    FILE_STATUS_FAILED,
    FILE_STATUS_SUCCESS,
    STATUS_COMPLETED,
    STATUS_FAILED,
    STATUS_IN_PROGRESS,
)
from migrator.s3_client import S3AuthError, S3Client, S3UploadError
from migrator.sftp_client import (
    FileInfo,
    SFTPClient,
    SFTPConnectionError,
    create_sftp_client,
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


def run_migration(
    config: AppConfig,
    folders_path: str,
    dry_run: bool = False,
) -> int:
    """Main entry point for the migration process.

    Processes folders sequentially. For each folder:
    - Uploads 'in_progress' tracking to S3
    - Transfers files concurrently
    - Uploads final tracking status to S3
    - Cleans up and marks folder as done

    Args:
        config: Application configuration.
        folders_path: Path to folders.txt (for marking completion).
        dry_run: If True, list files without transferring.

    Returns:
        Exit code (0 = success, 1 = some failures, 2 = fatal error).
    """
    stats = MigrationStats(start_time=time.time())

    if not config.folders:
        logger.info("No folders to migrate. Exiting.")
        print(stats.summary())
        return 0

    # Initialize S3 client early to catch auth errors before any work
    s3 = S3Client(config.s3)
    try:
        s3.connect()
    except S3AuthError as e:
        logger.critical("FATAL: S3 authentication failed: %s", e)
        return 2

    tracking_dir = _resolve_tracking_dir(config.base_dir)
    multipart_threshold = config.options.multipart_threshold_mb * 1024 * 1024

    for folder in config.folders:
        logger.info("=" * 60)
        logger.info("Processing folder: %s", folder)
        logger.info("=" * 60)

        try:
            _process_folder(
                folder=folder,
                config=config,
                s3=s3,
                tracking_dir=tracking_dir,
                folders_path=folders_path,
                multipart_threshold=multipart_threshold,
                dry_run=dry_run,
                stats=stats,
            )
        except FatalMigrationError as e:
            logger.critical("FATAL: %s", e)
            logger.critical("Halting migration — remaining folders will not be processed")
            print(stats.summary())
            return 2

    print(stats.summary())
    return 0 if stats.failed == 0 and stats.folders_failed == 0 else 1


def _resolve_tracking_dir(base_dir: str) -> str:
    """Resolve the tracking directory path."""
    import os
    tracking_dir = os.path.join(base_dir, "tracking")
    os.makedirs(tracking_dir, exist_ok=True)
    return tracking_dir


def _process_folder(
    folder: str,
    config: AppConfig,
    s3: S3Client,
    tracking_dir: str,
    folders_path: str,
    multipart_threshold: int,
    dry_run: bool,
    stats: MigrationStats,
) -> None:
    """Process a single folder through the full lifecycle.

    Raises:
        FatalMigrationError: On unrecoverable errors.
    """
    # Step 1: Load or create manifest
    manifest = Manifest.load_or_create(tracking_dir, folder)

    # Step 2: Connect to SFTP and list files
    try:
        with SFTPClient(config.sftp) as sftp:
            file_list = sftp.list_files(folder)
    except SFTPConnectionError as e:
        # Fatal: SFTP is down — no point continuing with other folders
        manifest.set_status(STATUS_FAILED)
        manifest.save()
        _try_upload_tracking(s3, manifest)
        raise FatalMigrationError(f"SFTP connection failed: {e}") from e

    logger.info("Found %d files in %s", len(file_list), folder)
    manifest.total_files = len(file_list)

    # Step 3: Filter out already-migrated files
    to_transfer = []
    for fi in file_list:
        if manifest.needs_transfer(fi.path, fi.size, fi.mtime):
            to_transfer.append(fi)
        else:
            stats.skipped += 1

    stats.total_files += len(file_list)

    logger.info(
        "%d files to transfer, %d already up-to-date",
        len(to_transfer),
        len(file_list) - len(to_transfer),
    )

    if not to_transfer:
        logger.info("Folder %s — nothing to transfer, marking as done", folder)
        manifest.set_status(STATUS_COMPLETED)
        manifest.save()
        _try_upload_tracking(s3, manifest)
        manifest.delete_local()
        mark_folder_done(folders_path, folder)
        stats.folders_completed += 1
        return

    # Step 4: Dry-run check
    if dry_run:
        logger.info("DRY RUN — would transfer %d files:", len(to_transfer))
        for fi in to_transfer:
            size_mb = fi.size / (1024 * 1024)
            logger.info("  %s (%.2f MB)", fi.path, size_mb)
        return

    # Step 5: Upload 'in_progress' tracking to S3
    manifest.set_status(STATUS_IN_PROGRESS)
    manifest.save()
    try:
        s3.upload_tracking(manifest.to_json(), folder)
    except S3AuthError as e:
        raise FatalMigrationError(f"S3 auth failed uploading tracking: {e}") from e
    except S3UploadError as e:
        raise FatalMigrationError(
            f"Cannot upload initial tracking to S3 (network issue?): {e}"
        ) from e

    # Step 6: Transfer files concurrently
    consecutive_failures = 0
    max_consecutive = config.options.consecutive_failure_threshold

    with ThreadPoolExecutor(max_workers=config.options.max_workers) as pool:
        futures = {}
        for fi in to_transfer:
            s3_key = fi.path.lstrip("/")
            future = pool.submit(
                _transfer_single_file,
                sftp_config=config.sftp,
                s3_client=s3,
                file_info=fi,
                s3_key=s3_key,
                multipart_threshold=multipart_threshold,
                multipart_chunk_mb=config.options.multipart_chunk_mb,
                max_retries=config.options.max_retries,
                retry_backoff_base=config.options.retry_backoff_base,
            )
            futures[future] = fi

        for future in as_completed(futures):
            fi = futures[future]
            try:
                bytes_sent = future.result()

                # Success
                manifest.record(
                    path=fi.path,
                    size=fi.size,
                    mtime=fi.mtime,
                    s3_key=fi.path.lstrip("/"),
                    status=FILE_STATUS_SUCCESS,
                )
                stats.transferred += 1
                stats.bytes_transferred += bytes_sent
                consecutive_failures = 0  # Reset on success

                logger.info(
                    "✓ %s (%.2f MB)",
                    fi.path,
                    fi.size / (1024 * 1024),
                )

            except S3AuthError as e:
                # Fatal — credentials expired mid-run
                manifest.record(
                    path=fi.path,
                    size=fi.size,
                    mtime=fi.mtime,
                    s3_key=fi.path.lstrip("/"),
                    status=FILE_STATUS_FAILED,
                )
                manifest.set_status(STATUS_FAILED)
                manifest.save()
                _try_upload_tracking(s3, manifest)
                # Cancel remaining futures
                for pending_future in futures:
                    pending_future.cancel()
                raise FatalMigrationError(
                    f"S3 auth error during transfer: {e}"
                ) from e

            except SFTPConnectionError as e:
                # Fatal — SFTP connection lost
                manifest.record(
                    path=fi.path,
                    size=fi.size,
                    mtime=fi.mtime,
                    s3_key=fi.path.lstrip("/"),
                    status=FILE_STATUS_FAILED,
                )
                manifest.set_status(STATUS_FAILED)
                manifest.save()
                _try_upload_tracking(s3, manifest)
                for pending_future in futures:
                    pending_future.cancel()
                raise FatalMigrationError(
                    f"SFTP connection lost during transfer: {e}"
                ) from e

            except Exception as e:
                # Per-file failure
                manifest.record(
                    path=fi.path,
                    size=fi.size,
                    mtime=fi.mtime,
                    s3_key=fi.path.lstrip("/"),
                    status=FILE_STATUS_FAILED,
                )
                stats.failed += 1
                consecutive_failures += 1

                logger.error("✗ %s — %s", fi.path, e)

                # Check consecutive failure threshold
                if consecutive_failures >= max_consecutive:
                    logger.critical(
                        "%d consecutive failures — likely systemic issue, halting",
                        consecutive_failures,
                    )
                    manifest.set_status(STATUS_FAILED)
                    manifest.save()
                    _try_upload_tracking(s3, manifest)
                    for pending_future in futures:
                        pending_future.cancel()
                    raise FatalMigrationError(
                        f"{consecutive_failures} consecutive file failures "
                        f"— systemic issue detected"
                    )

            # Periodically save manifest (every file, since it's fast)
            manifest.save()

    # Step 7: Finalize folder
    if manifest.all_successful():
        manifest.set_status(STATUS_COMPLETED)
        manifest.save()

        try:
            s3.upload_tracking(manifest.to_json(), folder)
        except (S3AuthError, S3UploadError) as e:
            logger.error("Failed to upload final tracking for %s: %s", folder, e)

        manifest.delete_local()
        mark_folder_done(folders_path, folder)
        stats.folders_completed += 1
        logger.info("✓ Folder completed: %s", folder)
    else:
        manifest.set_status(STATUS_FAILED)
        manifest.save()

        try:
            s3.upload_tracking(manifest.to_json(), folder)
        except (S3AuthError, S3UploadError) as e:
            logger.error("Failed to upload failed tracking for %s: %s", folder, e)

        stats.folders_failed += 1
        logger.warning(
            "✗ Folder has failures: %s (%d failed files — will retry on next run)",
            folder,
            manifest.failed_count,
        )


def _transfer_single_file(
    sftp_config: SFTPConfig,
    s3_client: S3Client,
    file_info: FileInfo,
    s3_key: str,
    multipart_threshold: int,
    multipart_chunk_mb: int,
    max_retries: int,
    retry_backoff_base: int,
) -> int:
    """Transfer a single file from SFTP to S3 with retries.

    Each worker thread gets its own SFTP connection since paramiko
    connections are not thread-safe.

    Args:
        sftp_config: SFTP connection config.
        s3_client: Shared S3 client (boto3 clients are thread-safe).
        file_info: File metadata (path, size, mtime).
        s3_key: Target S3 key.
        multipart_threshold: Size threshold for multipart upload.
        multipart_chunk_mb: Chunk size for multipart parts.
        max_retries: Maximum retry attempts.
        retry_backoff_base: Base for exponential backoff (seconds).

    Returns:
        Number of bytes transferred.

    Raises:
        S3AuthError: If S3 credentials are invalid (fatal).
        SFTPConnectionError: If SFTP connection fails (fatal).
        Exception: If all retries exhausted (per-file failure).
    """
    last_error: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        sftp = None
        try:
            sftp = create_sftp_client(sftp_config)
            file_obj = sftp.open_file(file_info.path)

            s3_client.upload_stream(
                file_obj=file_obj,
                s3_key=s3_key,
                size=file_info.size,
                multipart_threshold=multipart_threshold,
                multipart_chunk_mb=multipart_chunk_mb,
            )

            return file_info.size

        except (S3AuthError, SFTPConnectionError):
            # Fatal errors — re-raise immediately, don't retry
            raise

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
            if sftp:
                sftp.close()

    raise last_error or Exception(f"Transfer failed for {file_info.path}")


def _try_upload_tracking(s3: S3Client, manifest: Manifest) -> None:
    """Best-effort upload of tracking JSON to S3.

    Used during error handling — if this also fails, just log it.
    """
    try:
        s3.upload_tracking(manifest.to_json(), manifest.sftp_folder)
    except Exception as e:
        logger.error(
            "Failed to upload tracking (best-effort) for %s: %s",
            manifest.sftp_folder, e,
        )
