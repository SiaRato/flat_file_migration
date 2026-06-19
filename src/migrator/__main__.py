"""CLI entry point for the SFTP-to-S3 migrator.

Usage:
    python -m migrator --config config.yaml --folders folders.txt
    python -m migrator --dry-run
    python -m migrator --log-level DEBUG
"""

import argparse
import os
import sys

from migrator.config import AppConfig, ConfigError, load_config
from migrator.logging_config import setup_logging
from migrator.transfer import run_migration


def main() -> int:
    """Parse arguments and run the migration."""
    parser = argparse.ArgumentParser(
        prog="migrator",
        description="Migrate files from SFTP to S3",
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to YAML config file (default: config.yaml)",
    )
    parser.add_argument(
        "--folders",
        default="folders.txt",
        help="Path to folders list file (default: folders.txt)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be transferred without uploading",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )

    args = parser.parse_args()

    # Resolve base directory (deployment folder)
    # All relative paths are resolved against this directory
    base_dir = os.path.dirname(os.path.abspath(args.config))

    # Resolve config and folders paths
    config_path = os.path.abspath(args.config)
    folders_path = os.path.abspath(args.folders)

    # Load config first (before logging, since config has log_file setting)
    try:
        config = load_config(config_path, folders_path, base_dir)
    except ConfigError as e:
        # Can't use logger yet — print to stderr
        print(f"FATAL: Configuration error: {e}", file=sys.stderr)
        return 2

    # Setup logging
    setup_logging(
        level=args.log_level,
        log_file=config.options.log_file,
        base_dir=base_dir,
    )

    import logging
    logger = logging.getLogger("migrator")
    logger.info("SFTP-to-S3 Migrator starting")
    logger.info("Config: %s", config_path)
    logger.info("Folders: %s", folders_path)
    logger.info("Dry run: %s", args.dry_run)
    logger.info("Workers: %d", config.options.max_workers)
    logger.info(
        "Multipart threshold: %d MB", config.options.multipart_threshold_mb
    )
    logger.info("Folders to process: %d", len(config.folders))

    # Run migration
    exit_code = run_migration(
        config=config,
        folders_path=folders_path,
        dry_run=args.dry_run,
    )

    if exit_code == 0:
        logger.info("Migration completed successfully")
    elif exit_code == 1:
        logger.warning("Migration completed with some failures")
    else:
        logger.critical("Migration halted due to fatal error")

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
