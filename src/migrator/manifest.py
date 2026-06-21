"""Per-folder tracking manifest (Functional).

Manages an SQLite database that tracks the migration status of every file within
a single SFTP folder. Supports the status lifecycle:
    in_progress → completed | failed
"""

import logging
import os
import posixpath
import re
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Optional, List, Tuple

from migrator.types import ManifestContext
import migrator.s3_client as s3_client


logger = logging.getLogger("migrator.manifest")

# Overall migration status for a folder
STATUS_IN_PROGRESS = "in_progress"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"

# Per-file statuses
FILE_STATUS_SUCCESS = "success"
FILE_STATUS_FAILED = "failed"
FILE_STATUS_PENDING = "pending"


def load_or_create_manifest(
    tracking_dir: str, sftp_folder: str, s3_ctx: Optional[Any] = None
) -> ManifestContext:
    """Load an existing database from disk or S3, or create a new one."""
    os.makedirs(tracking_dir, exist_ok=True)
    filename = _sanitize_folder_name(sftp_folder) + ".db"
    tracking_path = os.path.join(tracking_dir, filename)

    if s3_ctx:
        try:
            s3_data_bytes = s3_client.download_tracking_bytes(s3_ctx, sftp_folder)
            if s3_data_bytes:
                with open(tracking_path, "wb") as f:
                    f.write(s3_data_bytes)
                logger.info("Downloaded and applied tracking DB from S3 for %s", sftp_folder)
        except Exception as e:
            logger.warning("Failed to fetch tracking DB from S3 for %s: %s", sftp_folder, e)

    lock = threading.Lock()
    conn = sqlite3.connect(tracking_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row

    ctx = ManifestContext(
        tracking_path=tracking_path,
        sftp_folder=sftp_folder,
        conn=conn,
        lock=lock,
    )

    _init_db(ctx)
    return ctx


def _init_db(ctx: ManifestContext):
    """Initialize SQLite database schema with optimizations."""
    with ctx.lock:
        cursor = ctx.conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL;")
        cursor.execute("PRAGMA synchronous=NORMAL;")
        
        # Create meta table for folder status
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        
        # Create files table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS files (
                path TEXT PRIMARY KEY,
                size INTEGER,
                mtime REAL,
                s3_key TEXT,
                migrated_at TEXT,
                status TEXT,
                seen INTEGER DEFAULT 0
            )
        """)
        
        # Create indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_seen_status ON files(seen, status)")
        
        ctx.conn.commit()

        # Initialize meta if empty
        cursor.execute("SELECT value FROM meta WHERE key = 'status'")
        if not cursor.fetchone():
            cursor.execute("INSERT INTO meta (key, value) VALUES ('status', ?)", (STATUS_IN_PROGRESS,))
            cursor.execute("INSERT INTO meta (key, value) VALUES ('started_at', ?)", (_now_iso(),))
            ctx.conn.commit()


def needs_transfer(ctx: ManifestContext, path: str, size: int, mtime: float) -> bool:
    """Check if a file needs to be transferred."""
    with ctx.lock:
        cursor = ctx.conn.cursor()
        cursor.execute("SELECT size, mtime, status FROM files WHERE path = ?", (path,))
        row = cursor.fetchone()
        
        if row is None:
            return True
        if row['status'] != FILE_STATUS_SUCCESS:
            return True
        if row['size'] != size or row['mtime'] != mtime:
            return True
        return False


def mark_seen(ctx: ManifestContext, path: str) -> None:
    """Mark a file as seen during the SFTP scan."""
    with ctx.lock:
        ctx.conn.execute("UPDATE files SET seen = 1 WHERE path = ?", (path,))


def record_transfer(
    ctx: ManifestContext,
    path: str,
    size: int,
    mtime: float,
    s3_key: str,
    status: str,
) -> None:
    """Record the result of a file transfer."""
    with ctx.lock:
        migrated_at = _now_iso() if status == FILE_STATUS_SUCCESS else None
        ctx.conn.execute("""
            INSERT OR REPLACE INTO files (path, size, mtime, s3_key, migrated_at, status, seen)
            VALUES (?, ?, ?, ?, ?, ?, 1)
        """, (path, size, mtime, s3_key, migrated_at, status))


def reset_seen_flags(ctx: ManifestContext) -> None:
    """Reset all seen flags to 0 at the start of a mirror sync."""
    with ctx.lock:
        ctx.conn.execute("UPDATE files SET seen = 0")
        ctx.conn.commit()


def get_unseen_success_files(ctx: ManifestContext) -> List[Tuple[str, str]]:
    """Return files that were successful but not seen in the current scan.
    Returns a list of (path, s3_key) tuples.
    """
    with ctx.lock:
        cursor = ctx.conn.cursor()
        cursor.execute("SELECT path, s3_key FROM files WHERE seen = 0 AND status = 'success'")
        return [(row['path'], row['s3_key']) for row in cursor.fetchall()]


def remove_file(ctx: ManifestContext, path: str) -> None:
    """Remove a file from the manifest."""
    with ctx.lock:
        ctx.conn.execute("DELETE FROM files WHERE path = ?", (path,))


def set_status(ctx: ManifestContext, status: str) -> None:
    """Set the overall migration status for this folder."""
    with ctx.lock:
        ctx.conn.execute("INSERT OR REPLACE INTO meta (key, value) VALUES ('status', ?)", (status,))
        if status in (STATUS_COMPLETED, STATUS_FAILED):
            ctx.conn.execute("INSERT OR REPLACE INTO meta (key, value) VALUES ('completed_at', ?)", (_now_iso(),))
        ctx.conn.commit()


def get_failed_count(ctx: ManifestContext) -> int:
    with ctx.lock:
        cursor = ctx.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM files WHERE status = 'failed'")
        return cursor.fetchone()[0]


def all_successful(ctx: ManifestContext) -> bool:
    """Check if all tracked files were migrated successfully."""
    return get_failed_count(ctx) == 0


def commit_manifest(ctx: ManifestContext) -> None:
    """Commit batched transactions to disk."""
    with ctx.lock:
        ctx.conn.commit()


def delete_local(ctx: ManifestContext) -> None:
    """Delete the local tracking file."""
    ctx.conn.close()
    if os.path.isfile(ctx.tracking_path):
        os.remove(ctx.tracking_path)
        # Delete WAL files too
        wal_path = ctx.tracking_path + "-wal"
        shm_path = ctx.tracking_path + "-shm"
        if os.path.isfile(wal_path):
            os.remove(wal_path)
        if os.path.isfile(shm_path):
            os.remove(shm_path)
        logger.info("Deleted local tracking DB: %s", ctx.tracking_path)


def close_manifest(ctx: ManifestContext):
    """Close DB connection."""
    ctx.conn.close()


def _sanitize_folder_name(folder: str) -> str:
    """Convert an SFTP folder path to a safe filename."""
    norm = posixpath.normpath(folder).lstrip("/")
    sanitized = norm.replace("/", "__")
    sanitized = re.sub(r"[^\w\-.]", "_", sanitized)
    return sanitized or "root"


def _now_iso() -> str:
    """Return current UTC time as ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat()
