"""Context types for functional state management.

These dataclasses replace stateful class instances to pass explicit context 
to functional modules.
"""

from dataclasses import dataclass
from typing import Any, Optional

from migrator.config import S3Config, SFTPConfig


@dataclass
class S3Context:
    """Holds state for S3 operations."""
    config: S3Config
    client: Any  # boto3.client


@dataclass
class SFTPContext:
    """Holds state for SFTP operations."""
    config: SFTPConfig
    transport: Any  # paramiko.Transport
    sftp: Any  # paramiko.SFTPClient


@dataclass
class ManifestContext:
    """Holds state for local folder tracking DB."""
    tracking_path: str
    sftp_folder: str
    conn: Any  # sqlite3.Connection
    lock: Any  # threading.Lock


@dataclass
class SchedulerContext:
    """Holds state for the daemon scheduler DB."""
    db_path: str
    conn: Any  # sqlite3.Connection
