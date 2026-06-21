"""Configuration loading and validation.

Loads config.yaml, parses folders.txt (with deduplication of nested paths),
and merges environment variables for secrets.
"""

import logging
import os
import posixpath
from dataclasses import dataclass, field
from typing import List, Optional

import yaml


logger = logging.getLogger("migrator.config")


class ConfigError(Exception):
    """Raised when configuration is invalid or missing."""
    pass


@dataclass
class SFTPConfig:
    host: str
    port: int
    username: str
    password: str


@dataclass
class S3Config:
    bucket: str
    prefix: str
    region: str
    tracking_prefix: str


@dataclass
class OptionsConfig:
    max_workers: int = 4
    multipart_threshold_mb: int = 100
    multipart_chunk_mb: int = 16
    max_retries: int = 3
    retry_backoff_base: int = 2
    consecutive_failure_threshold: int = 5
    log_file: Optional[str] = None


@dataclass
class FolderJob:
    path: str
    cron_expr: Optional[str] = None


@dataclass
class AppConfig:
    sftp: SFTPConfig
    s3: S3Config
    options: OptionsConfig
    folders: List[FolderJob] = field(default_factory=list)
    base_dir: str = ""


def load_config(config_path: str, folders_path: str, base_dir: str, folder_override: Optional[str] = None) -> AppConfig:
    """Load and validate the application configuration.

    Args:
        config_path: Absolute path to config.yaml.
        folders_path: Absolute path to folders.txt.
        base_dir: Deployment base directory (for resolving relative paths).
        folder_override: Optional single folder to run instead of reading folders.txt.

    Returns:
        Validated AppConfig instance.

    Raises:
        ConfigError: If config is missing or invalid.
    """
    # Load YAML config
    if not os.path.isfile(config_path):
        raise ConfigError(f"Config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        raise ConfigError(f"Invalid config format in {config_path}")

    # Parse SFTP config
    sftp_raw = raw.get("sftp", {})
    sftp_password = os.environ.get("SFTP_PASSWORD", "")
    if not sftp_password:
        raise ConfigError("SFTP_PASSWORD environment variable is not set")

    sftp = SFTPConfig(
        host=sftp_raw.get("host", ""),
        port=sftp_raw.get("port", 22),
        username=sftp_raw.get("username", ""),
        password=sftp_password,
    )

    if not sftp.host or not sftp.username:
        raise ConfigError("SFTP host and username are required in config.yaml")

    # Parse S3 config
    s3_raw = raw.get("s3", {})
    s3 = S3Config(
        bucket=s3_raw.get("bucket", ""),
        prefix=s3_raw.get("prefix", ""),
        region=s3_raw.get("region", "us-east-1"),
        tracking_prefix=s3_raw.get("tracking_prefix", "migration-tracking"),
    )

    if not s3.bucket:
        raise ConfigError("S3 bucket is required in config.yaml")

    # Parse options
    opts_raw = raw.get("options", {})
    options = OptionsConfig(
        max_workers=opts_raw.get("max_workers", 4),
        multipart_threshold_mb=opts_raw.get("multipart_threshold_mb", 100),
        multipart_chunk_mb=opts_raw.get("multipart_chunk_mb", 16),
        max_retries=opts_raw.get("max_retries", 3),
        retry_backoff_base=opts_raw.get("retry_backoff_base", 2),
        consecutive_failure_threshold=opts_raw.get("consecutive_failure_threshold", 5),
        log_file=opts_raw.get("log_file"),
    )

    # Parse folders.txt or use override
    if folder_override:
        folders = [FolderJob(path=folder_override, cron_expr=None)]
    else:
        folders = _parse_folders(folders_path)

    return AppConfig(
        sftp=sftp,
        s3=s3,
        options=options,
        folders=folders,
        base_dir=base_dir,
    )


def _parse_folders(folders_path: str) -> List[FolderJob]:
    """Parse folders.txt, skipping blanks and DONE lines, then deduplicate.

    Args:
        folders_path: Path to the folders.txt file.

    Returns:
        Deduplicated list of FolderJob instances.

    Raises:
        ConfigError: If folders.txt is missing or contains invalid cron expressions.
    """
    if not os.path.isfile(folders_path):
        raise ConfigError(f"Folders file not found: {folders_path}")

    try:
        from croniter import croniter
    except ImportError:
        croniter = None
        logger.warning("croniter is not installed, cron expressions will not be parsed.")

    with open(folders_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    raw_jobs = []
    for line in lines:
        stripped = line.strip()
        # Skip blank lines, comments, and completed folders
        if not stripped or stripped.startswith("#"):
            continue

        parts = stripped.split()
        if len(parts) >= 6:
            potential_cron = " ".join(parts[:5])
            path = " ".join(parts[5:])
            if croniter and croniter.is_valid(potential_cron):
                raw_jobs.append(FolderJob(path=path, cron_expr=potential_cron))
                continue
            elif croniter and not croniter.is_valid(potential_cron):
                # We know croniter is installed, and it rejected the syntax. Let's raise an error.
                # However, maybe the user just has a directory path with spaces that happens to have 6 words.
                # We will check if the first 5 parts are strictly cron-like.
                # A hard fail is best here for validation.
                raise ConfigError(f"Invalid cron expression in folders.txt: '{potential_cron}' for path '{path}'")

        # No cron prefix found or croniter missing, treat as one-time job
        raw_jobs.append(FolderJob(path=stripped, cron_expr=None))

    if not raw_jobs:
        logger.info("No pending folders found in %s — nothing to migrate", folders_path)
        return []

    # Deduplicate nested folders
    deduped = _deduplicate_nested(raw_jobs)

    logger.info(
        "Loaded %d folder(s) to migrate (%d removed as nested duplicates)",
        len(deduped),
        len(raw_jobs) - len(deduped),
    )

    return deduped


def _deduplicate_nested(jobs: List[FolderJob]) -> List[FolderJob]:
    """Remove folders that are subfolders of other folders in the list.

    Since the script traverses recursively, a child folder is already
    covered by its parent. Keeps only top-level (non-overlapping) folders.

    Args:
        jobs: Raw list of FolderJobs.

    Returns:
        Deduplicated list with only top-level FolderJobs.
    """
    # Normalize paths
    for j in jobs:
        j.path = posixpath.normpath(j.path)

    # Sort by path length (shortest first), then alphabetically for stability
    jobs.sort(key=lambda j: (len(j.path), j.path))

    kept = []
    for job in jobs:
        is_nested = False
        for parent in kept:
            # Check if folder is a subfolder of parent
            if job.path == parent.path:
                is_nested = True
                logger.warning(
                    "Skipping duplicate folder: %s", job.path
                )
                break
            if job.path.startswith(parent.path + "/"):
                is_nested = True
                logger.warning(
                    "Skipping %s — already covered by parent %s", job.path, parent.path
                )
                break

        if not is_nested:
            kept.append(job)

    return kept



