"""Per-folder tracking manifest.

Manages a JSON file that tracks the migration status of every file within
a single SFTP folder. Supports the status lifecycle:
    in_progress → completed | failed
"""

import json
import logging
import os
import posixpath
import re
import threading
from datetime import datetime, timezone
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, Optional


logger = logging.getLogger("migrator.manifest")


# Overall migration status for a folder
STATUS_IN_PROGRESS = "in_progress"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"

# Per-file statuses
FILE_STATUS_SUCCESS = "success"
FILE_STATUS_FAILED = "failed"
FILE_STATUS_PENDING = "pending"


@dataclass
class FileEntry:
    """Tracking entry for a single file."""
    size: int
    mtime: float
    s3_key: str
    migrated_at: Optional[str] = None
    status: str = FILE_STATUS_PENDING


class Manifest:
    """Per-folder tracking manifest.

    Thread-safe: uses a lock for all mutations so concurrent workers
    can safely call record().
    """

    def __init__(self, tracking_path: str, sftp_folder: str):
        self._tracking_path = tracking_path
        self._sftp_folder = sftp_folder
        self._lock = threading.Lock()

        self.version: int = 1
        self.status: str = STATUS_IN_PROGRESS
        self.started_at: str = _now_iso()
        self.completed_at: Optional[str] = None
        self.total_files: int = 0
        self.migrated_count: int = 0
        self.failed_count: int = 0
        self.files: Dict[str, FileEntry] = {}

    @property
    def tracking_path(self) -> str:
        """Local file path for this manifest."""
        return self._tracking_path

    @property
    def sftp_folder(self) -> str:
        """The SFTP folder this manifest tracks."""
        return self._sftp_folder

    @classmethod
    def load_or_create(
        cls, tracking_dir: str, sftp_folder: str
    ) -> "Manifest":
        """Load an existing manifest from disk or create a new one.

        Args:
            tracking_dir: Directory where tracking JSON files are stored.
            sftp_folder: The SFTP folder path this manifest tracks.

        Returns:
            A Manifest instance (loaded from disk if available).
        """
        os.makedirs(tracking_dir, exist_ok=True)
        filename = _sanitize_folder_name(sftp_folder) + ".json"
        tracking_path = os.path.join(tracking_dir, filename)

        manifest = cls(tracking_path=tracking_path, sftp_folder=sftp_folder)

        if os.path.isfile(tracking_path):
            try:
                with open(tracking_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                manifest.version = data.get("version", 1)
                manifest.status = data.get("status", STATUS_IN_PROGRESS)
                manifest.started_at = data.get("started_at", _now_iso())
                manifest.completed_at = data.get("completed_at")
                manifest.total_files = data.get("total_files", 0)
                manifest.migrated_count = data.get("migrated_count", 0)
                manifest.failed_count = data.get("failed_count", 0)

                for path, entry_data in data.get("files", {}).items():
                    manifest.files[path] = FileEntry(
                        size=entry_data.get("size", 0),
                        mtime=entry_data.get("mtime", 0),
                        s3_key=entry_data.get("s3_key", ""),
                        migrated_at=entry_data.get("migrated_at"),
                        status=entry_data.get("status", FILE_STATUS_PENDING),
                    )

                logger.info(
                    "Loaded existing manifest for %s — %d files tracked "
                    "(%d successful, %d failed)",
                    sftp_folder,
                    len(manifest.files),
                    manifest.migrated_count,
                    manifest.failed_count,
                )
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                logger.warning(
                    "Corrupt manifest at %s, starting fresh: %s",
                    tracking_path, e,
                )
                manifest = cls(tracking_path=tracking_path, sftp_folder=sftp_folder)
        else:
            logger.info("Creating new manifest for folder: %s", sftp_folder)

        return manifest

    def needs_transfer(self, path: str, size: int, mtime: float) -> bool:
        """Check if a file needs to be transferred.

        A file is skipped if it already exists in the manifest with
        status=success AND the same size+mtime.

        Args:
            path: SFTP file path.
            size: File size in bytes.
            mtime: File modification time (epoch).

        Returns:
            True if the file needs to be transferred.
        """
        with self._lock:
            entry = self.files.get(path)
            if entry is None:
                return True
            if entry.status != FILE_STATUS_SUCCESS:
                return True
            # Re-transfer if file has changed
            if entry.size != size or entry.mtime != mtime:
                return True
            return False

    def record(
        self,
        path: str,
        size: int,
        mtime: float,
        s3_key: str,
        status: str,
    ) -> None:
        """Record the result of a file transfer.

        Args:
            path: SFTP file path.
            size: File size in bytes.
            mtime: File modification time (epoch).
            s3_key: The S3 key the file was uploaded to.
            status: FILE_STATUS_SUCCESS or FILE_STATUS_FAILED.
        """
        with self._lock:
            old_entry = self.files.get(path)

            # Update counters: undo previous status if re-recording
            if old_entry:
                if old_entry.status == FILE_STATUS_SUCCESS:
                    self.migrated_count = max(0, self.migrated_count - 1)
                elif old_entry.status == FILE_STATUS_FAILED:
                    self.failed_count = max(0, self.failed_count - 1)

            self.files[path] = FileEntry(
                size=size,
                mtime=mtime,
                s3_key=s3_key,
                migrated_at=_now_iso() if status == FILE_STATUS_SUCCESS else None,
                status=status,
            )

            if status == FILE_STATUS_SUCCESS:
                self.migrated_count += 1
            elif status == FILE_STATUS_FAILED:
                self.failed_count += 1

    def set_status(self, status: str) -> None:
        """Set the overall migration status for this folder.

        Args:
            status: STATUS_IN_PROGRESS, STATUS_COMPLETED, or STATUS_FAILED.
        """
        with self._lock:
            self.status = status
            if status in (STATUS_COMPLETED, STATUS_FAILED):
                self.completed_at = _now_iso()

    def all_successful(self) -> bool:
        """Check if all tracked files were migrated successfully."""
        with self._lock:
            if not self.files:
                return True
            return all(
                entry.status == FILE_STATUS_SUCCESS
                for entry in self.files.values()
            )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize manifest to a dictionary."""
        with self._lock:
            return {
                "version": self.version,
                "sftp_folder": self._sftp_folder,
                "status": self.status,
                "started_at": self.started_at,
                "completed_at": self.completed_at,
                "total_files": self.total_files,
                "migrated_count": self.migrated_count,
                "failed_count": self.failed_count,
                "files": {
                    path: asdict(entry)
                    for path, entry in self.files.items()
                },
            }

    def to_json(self) -> str:
        """Serialize manifest to a JSON string."""
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False)

    def save(self) -> None:
        """Write manifest to disk."""
        data = self.to_json()
        # Write atomically: write to temp then rename
        tmp_path = self._tracking_path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(data)
        os.replace(tmp_path, self._tracking_path)
        logger.debug("Saved manifest to %s", self._tracking_path)

    def delete_local(self) -> None:
        """Delete the local tracking file."""
        if os.path.isfile(self._tracking_path):
            os.remove(self._tracking_path)
            logger.info("Deleted local tracking file: %s", self._tracking_path)


def _sanitize_folder_name(folder: str) -> str:
    """Convert an SFTP folder path to a safe filename.

    Example: /data/reports → data__reports
    """
    # Normalize and strip leading /
    norm = posixpath.normpath(folder).lstrip("/")
    # Replace / with __
    sanitized = norm.replace("/", "__")
    # Remove any chars that aren't alphanumeric, underscore, hyphen, or dot
    sanitized = re.sub(r"[^\w\-.]", "_", sanitized)
    return sanitized or "root"


def _now_iso() -> str:
    """Return current UTC time as ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat()
