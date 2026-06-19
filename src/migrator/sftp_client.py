"""SFTP client wrapper.

Provides connection management, recursive directory listing,
and file streaming via paramiko.
"""

import logging
import stat
from dataclasses import dataclass
from typing import IO, List, Optional

import paramiko

from migrator.config import SFTPConfig


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


class SFTPClient:
    """Wrapper around paramiko for SFTP operations.

    Each instance maintains its own transport + SFTP session.
    Use as a context manager or call connect()/close() manually.
    """

    def __init__(self, config: SFTPConfig):
        self._config = config
        self._transport: Optional[paramiko.Transport] = None
        self._sftp: Optional[paramiko.SFTPClient] = None

    def __enter__(self) -> "SFTPClient":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def connect(self) -> None:
        """Establish SFTP connection.

        Raises:
            SFTPConnectionError: If connection or authentication fails.
        """
        try:
            logger.info(
                "Connecting to SFTP %s@%s:%d",
                self._config.username,
                self._config.host,
                self._config.port,
            )
            self._transport = paramiko.Transport(
                (self._config.host, self._config.port)
            )
            self._transport.connect(
                username=self._config.username,
                password=self._config.password,
            )
            self._sftp = paramiko.SFTPClient.from_transport(self._transport)
            logger.info("SFTP connection established successfully")
        except paramiko.AuthenticationException as e:
            raise SFTPConnectionError(
                f"SFTP authentication failed for {self._config.username}@"
                f"{self._config.host}: {e}"
            ) from e
        except Exception as e:
            raise SFTPConnectionError(
                f"Failed to connect to SFTP {self._config.host}:"
                f"{self._config.port}: {e}"
            ) from e

    def close(self) -> None:
        """Close the SFTP connection."""
        if self._sftp:
            try:
                self._sftp.close()
            except Exception:
                pass
            self._sftp = None
        if self._transport:
            try:
                self._transport.close()
            except Exception:
                pass
            self._transport = None
        logger.debug("SFTP connection closed")

    @property
    def is_connected(self) -> bool:
        """Check if the connection is still active."""
        return (
            self._transport is not None
            and self._transport.is_active()
            and self._sftp is not None
        )

    def list_files(self, folder: str) -> List[FileInfo]:
        """Recursively list all files in a remote folder.

        Args:
            folder: Remote folder path to list.

        Returns:
            List of FileInfo for all files found recursively.

        Raises:
            SFTPConnectionError: If listing fails due to connection issues.
        """
        if not self._sftp:
            raise SFTPConnectionError("Not connected to SFTP server")

        files: List[FileInfo] = []
        self._walk(folder, files)
        return files

    def _walk(self, path: str, files: List[FileInfo]) -> None:
        """Recursively walk a remote directory tree."""
        try:
            entries = self._sftp.listdir_attr(path)
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
                self._walk(full_path, files)
            elif stat.S_ISREG(entry.st_mode):
                files.append(
                    FileInfo(
                        path=full_path,
                        size=entry.st_size,
                        mtime=entry.st_mtime,
                    )
                )

    def open_file(self, path: str) -> IO[bytes]:
        """Open a remote file for reading.

        Args:
            path: Remote file path.

        Returns:
            A file-like object for streaming reads.

        Raises:
            SFTPConnectionError: If the file cannot be opened.
        """
        if not self._sftp:
            raise SFTPConnectionError("Not connected to SFTP server")

        try:
            # prefetch=True enables read-ahead for better streaming performance
            f = self._sftp.open(path, "rb")
            f.prefetch()
            return f
        except Exception as e:
            raise IOError(f"Failed to open remote file {path}: {e}") from e


def create_sftp_client(config: SFTPConfig) -> SFTPClient:
    """Factory function to create and connect an SFTP client.

    Args:
        config: SFTP configuration.

    Returns:
        Connected SFTPClient instance.

    Raises:
        SFTPConnectionError: If connection fails (fatal).
    """
    client = SFTPClient(config)
    client.connect()
    return client
