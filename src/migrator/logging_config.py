"""Logging configuration for the migrator.

Sets up structured console logging with optional file output.
"""

import logging
import os
import sys
from typing import Optional


LOG_FORMAT = "[%(asctime)s] %(levelname)-8s %(name)s — %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logging(
    level: str = "INFO",
    log_file: Optional[str] = None,
    base_dir: Optional[str] = None,
) -> logging.Logger:
    """Configure the root migrator logger.

    Args:
        level: Log level string (DEBUG, INFO, WARNING, ERROR).
        log_file: Optional path to a log file. If relative, resolved
                  against base_dir.
        base_dir: Base directory for resolving relative log_file paths.

    Returns:
        The configured root logger for the migrator package.
    """
    logger = logging.getLogger("migrator")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Prevent duplicate handlers on re-init
    logger.handlers.clear()

    # Console handler — always on
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT))
    logger.addHandler(console_handler)

    # File handler — optional
    if log_file:
        if base_dir and not os.path.isabs(log_file):
            log_file = os.path.join(base_dir, log_file)

        # Ensure log directory exists
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)

        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT))
        logger.addHandler(file_handler)

    return logger
