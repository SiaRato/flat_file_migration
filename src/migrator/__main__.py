"""CLI entry point for the SFTP-to-S3 migrator.

Usage:
    python -m migrator --config config.yaml --folders folders.yaml
    python -m migrator --dry-run
    python -m migrator --folder /path/to/specific --target my/s3/target
    python -m migrator --log-level DEBUG
"""

import argparse
import os
import sys

from migrator.config import AppConfig, ConfigError, load_config
from migrator.logging_config import setup_logging


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
        default="folders.yaml",
        help="Path to folders list file (default: folders.yaml)",
    )
    parser.add_argument(
        "--folder",
        default=None,
        help="Override folders.yaml and run on a single specific folder once. Requires --target.",
    )
    parser.add_argument(
        "--target",
        default=None,
        help="S3 target path to use with --folder.",
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
        config = load_config(config_path, folders_path, base_dir, args.folder, args.target)
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

    if args.folder:
        from migrator.s3_client import create_s3_context, S3AuthError
        from migrator.transfer import _process_folder, _resolve_tracking_dir, MigrationStats, FatalMigrationError
        import time
        
        # Single folder override mode
        try:
            s3_ctx = create_s3_context(config.s3)
        except S3AuthError as e:
            logger.critical("FATAL: S3 authentication failed: %s", e)
            return 2
            
        tracking_dir = _resolve_tracking_dir(config.base_dir)
        multipart_threshold = config.options.multipart_threshold_mb * 1024 * 1024
        stats = MigrationStats(start_time=time.time())
        
        try:
            _process_folder(
                folder=args.folder,
                target=config.folders[0].target,
                config=config,
                s3_ctx=s3_ctx,
                tracking_dir=tracking_dir,
                folders_path=folders_path,
                multipart_threshold=multipart_threshold,
                dry_run=args.dry_run,
                stats=stats,
            )
            exit_code = 0 if stats.failed == 0 else 1
            logger.info("Manual folder override completed")
            print(stats.summary())
        except FatalMigrationError as e:
            logger.critical("FATAL: %s", e)
            print(stats.summary())
            exit_code = 2
            
    else:
        # Default behavior: run the daemon scheduler
        from migrator.scheduler import run_daemon
        exit_code = run_daemon(
            config=config,
            folders_path=folders_path,
            dry_run=args.dry_run,
        )

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
