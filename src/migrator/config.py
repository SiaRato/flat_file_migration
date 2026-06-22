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
    target: str
    cron_expr: Optional[str] = None
    mirror_deletions: bool = True
    filename_template: Optional[str] = None
    schedule_lookback_days: int = 15
    initial_lookback_days: int = 7300


@dataclass
class AppConfig:
    sftp: SFTPConfig
    s3: S3Config
    options: OptionsConfig
    folders: List[FolderJob] = field(default_factory=list)
    base_dir: str = ""


def load_config(config_path: str, folders_path: str, base_dir: str, folder_override: Optional[str] = None, target_override: Optional[str] = None) -> AppConfig:
    """Load and validate the application configuration.

    Args:
        config_path: Absolute path to config.yaml.
        folders_path: Absolute path to folders.yaml.
        base_dir: Deployment base directory (for resolving relative paths).
        folder_override: Optional single folder to run instead of reading folders.yaml.
        target_override: Optional single target to use when overriding.

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

    # Parse folders.yaml or use override
    if folder_override:
        if not target_override:
            raise ConfigError("target_override must be provided if folder_override is used")
        folders = [FolderJob(path=folder_override, target=target_override, cron_expr=None)]
    else:
        folders = _parse_folders_yaml(folders_path)

    return AppConfig(
        sftp=sftp,
        s3=s3,
        options=options,
        folders=folders,
        base_dir=base_dir,
    )


def _parse_folders_yaml(folders_path: str) -> List[FolderJob]:
    """Parse folders.yaml and validate.

    Args:
        folders_path: Path to the folders.yaml file.

    Returns:
        Validated list of FolderJob instances.

    Raises:
        ConfigError: If folders.yaml is missing, invalid, or contains overlapping folders.
    """
    if not os.path.isfile(folders_path):
        raise ConfigError(f"Folders file not found: {folders_path}")

    try:
        from croniter import croniter
    except ImportError:
        croniter = None
        logger.warning("croniter is not installed, cron expressions will not be parsed.")

    with open(folders_path, "r", encoding="utf-8") as f:
        try:
            raw = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ConfigError(f"Invalid YAML format in {folders_path}: {e}")

    if not raw or not isinstance(raw, dict) or "folders" not in raw:
        logger.info("No 'folders' list found in %s — nothing to migrate", folders_path)
        return []

    folder_list = raw.get("folders", [])
    if not isinstance(folder_list, list):
        raise ConfigError("'folders' must be a list in YAML config")

    raw_jobs = []
    for item in folder_list:
        if not isinstance(item, dict):
            raise ConfigError(f"Invalid folder entry in YAML: {item}")
        
        source = item.get("source")
        target = item.get("target")
        cron = item.get("cron")
        mirror_deletions = item.get("mirror_deletions", True)
        filename_template = item.get("filename_template")
        schedule_lookback_days = item.get("schedule_lookback_days", 15)
        initial_lookback_days = item.get("initial_lookback_days", 7300)

        if not source or not target:
            raise ConfigError(f"Folder entry missing required 'source' or 'target': {item}")

        if cron and croniter and not croniter.is_valid(cron):
            raise ConfigError(f"Invalid cron expression '{cron}' for path '{source}'")

        raw_jobs.append(FolderJob(
            path=str(source),
            target=str(target),
            cron_expr=str(cron) if cron else None,
            mirror_deletions=bool(mirror_deletions),
            filename_template=str(filename_template) if filename_template else None,
            schedule_lookback_days=int(schedule_lookback_days),
            initial_lookback_days=int(initial_lookback_days)
        ))

    if not raw_jobs:
        logger.info("No pending folders found in %s — nothing to migrate", folders_path)
        return []

    # Validate nested folders
    valid_jobs = _validate_no_overlaps(raw_jobs)

    logger.info("Loaded %d folder(s) to migrate", len(valid_jobs))

    return valid_jobs


def _validate_no_overlaps(jobs: List[FolderJob]) -> List[FolderJob]:
    """Ensure no folders overlap with each other.

    Args:
        jobs: Raw list of FolderJobs.

    Returns:
        Validated list of FolderJobs.

    Raises:
        ConfigError: If any folder is a subfolder of another, or if there are duplicates.
    """
    # Normalize paths
    for j in jobs:
        j.path = posixpath.normpath(j.path)

    # Sort by path length (shortest first), then alphabetically for stability
    jobs.sort(key=lambda j: (len(j.path), j.path))

    kept = []
    for job in jobs:
        for parent in kept:
            # Check if folder is a subfolder of parent
            if job.path == parent.path:
                raise ConfigError(f"Duplicate folder source detected: '{job.path}'")
            if job.path.startswith(parent.path + "/"):
                raise ConfigError(f"Overlapping folders detected: '{job.path}' is covered by parent '{parent.path}'")

        kept.append(job)

    return kept



