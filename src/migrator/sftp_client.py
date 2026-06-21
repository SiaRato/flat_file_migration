"""SFTP client wrapper (Functional).

Provides connection management, recursive directory listing,
and file streaming via paramiko.
"""

import logging
import stat
from dataclasses import dataclass
from typing import IO, List, Optional, Generator

import paramiko

from migrator.config import SFTPConfig
from migrator.types import SFTPContext


logger = logging.getLogger("migrator.sftp")


class SFTPConnectionError(Exception):
    """Fatal: cannot connect to the SFTP server."""
    pass


@dataclass
class FileInfo:
    """Metadata for a remote file."""
    path: str
    size: int
    mtime: float


def create_sftp_context(config: SFTPConfig) -> SFTPContext:
    """Establish SFTP connection and return context.

    Raises:
        SFTPConnectionError: If connection or authentication fails.
    """
    try:
        logger.info(
            "Connecting to SFTP %s@%s:%d",
            config.username,
            config.host,
            config.port,
        )
        transport = paramiko.Transport((config.host, config.port))
        transport.connect(
            username=config.username,
            password=config.password,
        )
        sftp = paramiko.SFTPClient.from_transport(transport)
        logger.info("SFTP connection established successfully")
        return SFTPContext(config=config, transport=transport, sftp=sftp)
    except paramiko.AuthenticationException as e:
        raise SFTPConnectionError(
            f"SFTP authentication failed for {config.username}@"
            f"{config.host}: {e}"
        ) from e
    except Exception as e:
        raise SFTPConnectionError(
            f"Failed to connect to SFTP {config.host}:"
            f"{config.port}: {e}"
        ) from e


def close_sftp_context(ctx: SFTPContext) -> None:
    """Close the SFTP connection."""
    if ctx.sftp:
        try:
            ctx.sftp.close()
        except Exception:
            pass
        ctx.sftp = None
    if ctx.transport:
        try:
            ctx.transport.close()
        except Exception:
            pass
        ctx.transport = None
    logger.debug("SFTP connection closed")


def iter_files(ctx: SFTPContext, folder: str) -> Generator[FileInfo, None, None]:
    """Recursively yield all files in a remote folder."""
    if not ctx.sftp:
        raise SFTPConnectionError("Not connected to SFTP server")

    yield from _iter_walk(ctx, folder)


def _iter_walk(ctx: SFTPContext, path: str) -> Generator[FileInfo, None, None]:
    """Recursively walk a remote directory tree."""
    try:
        entries = ctx.sftp.listdir_attr(path)
    except FileNotFoundError:
        logger.warning("Remote directory not found: %s", path)
        return
    except IOError as e:
        logger.error("Error listing directory %s: %s", path, e)
        raise SFTPConnectionError(
            f"Failed to list directory {path}: {e}"
        ) from e

    for entry in entries:
        full_path = f"{path.rstrip('/')}/{entry.filename}"

        if stat.S_ISDIR(entry.st_mode):
            yield from _iter_walk(ctx, full_path)
        elif stat.S_ISREG(entry.st_mode):
            yield FileInfo(
                    path=full_path,
                    size=entry.st_size,
                    mtime=entry.st_mtime,
                )


def open_file(ctx: SFTPContext, path: str) -> IO[bytes]:
    """Open a remote file for reading."""
    if not ctx.sftp:
        raise SFTPConnectionError("Not connected to SFTP server")

    try:
        # prefetch=True enables read-ahead for better streaming performance
        f = ctx.sftp.open(path, "rb")
        f.prefetch()
        return f
    except Exception as e:
        raise IOError(f"Failed to open remote file {path}: {e}") from e
