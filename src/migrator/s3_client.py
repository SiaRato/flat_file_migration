"""S3 client wrapper.

Provides streaming uploads (regular and multipart) and tracking file uploads
via boto3.
"""

import io
import logging
from typing import IO, Optional

import boto3
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    NoCredentialsError,
    PartialCredentialsError,
)

from migrator.config import S3Config


logger = logging.getLogger("migrator.s3")

# 8 MB read buffer for streaming uploads
STREAM_BUFFER_SIZE = 8 * 1024 * 1024


class S3AuthError(Exception):
    """Fatal: S3 credentials are invalid or missing."""
    pass


class S3UploadError(Exception):
    """Non-fatal: a single upload failed."""
    pass


class S3Client:
    """Wrapper around boto3 S3 client."""

    def __init__(self, config: S3Config):
        self._config = config
        self._client = None

    def connect(self) -> None:
        """Initialize the boto3 S3 client.

        Raises:
            S3AuthError: If AWS credentials are missing or invalid.
        """
        try:
            self._client = boto3.client(
                "s3",
                region_name=self._config.region,
            )
            # Validate credentials with a lightweight call
            self._client.head_bucket(Bucket=self._config.bucket)
            logger.info(
                "S3 connection validated — bucket: %s, region: %s",
                self._config.bucket,
                self._config.region,
            )
        except (NoCredentialsError, PartialCredentialsError) as e:
            raise S3AuthError(
                f"AWS credentials not found or incomplete: {e}"
            ) from e
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code in ("403", "InvalidAccessKeyId", "SignatureDoesNotMatch"):
                raise S3AuthError(
                    f"S3 access denied for bucket {self._config.bucket}: {e}"
                ) from e
            elif error_code == "404":
                raise S3AuthError(
                    f"S3 bucket not found: {self._config.bucket}"
                ) from e
            raise S3AuthError(
                f"S3 connection error: {e}"
            ) from e

    def _build_key(self, relative_path: str) -> str:
        """Build the full S3 key from a relative path.

        Strips leading '/' from the path and prepends the configured prefix.
        """
        clean_path = relative_path.lstrip("/")
        if self._config.prefix:
            return f"{self._config.prefix.strip('/')}/{clean_path}"
        return clean_path

    def upload_stream(
        self,
        file_obj: IO[bytes],
        s3_key: str,
        size: int,
        multipart_threshold: int,
        multipart_chunk_mb: int,
    ) -> None:
        """Upload a file stream to S3.

        Chooses between regular put_object and multipart upload based on
        the file size.

        Args:
            file_obj: File-like object to read from.
            s3_key: Target S3 key.
            size: File size in bytes.
            multipart_threshold: Size threshold in bytes for multipart.
            multipart_chunk_mb: Chunk size in MB for multipart parts.

        Raises:
            S3AuthError: If credentials are invalid (fatal).
            S3UploadError: If the upload fails (per-file, retriable).
        """
        full_key = self._build_key(s3_key)

        try:
            if size <= multipart_threshold:
                self._upload_regular(file_obj, full_key, size)
            else:
                self._upload_multipart(file_obj, full_key, size, multipart_chunk_mb)
        except (NoCredentialsError, PartialCredentialsError) as e:
            raise S3AuthError(f"AWS credentials error during upload: {e}") from e
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code in ("403", "InvalidAccessKeyId", "SignatureDoesNotMatch"):
                raise S3AuthError(f"S3 access denied: {e}") from e
            raise S3UploadError(
                f"Failed to upload {full_key}: {e}"
            ) from e
        except (BotoCoreError, IOError) as e:
            raise S3UploadError(
                f"Failed to upload {full_key}: {e}"
            ) from e

    def _upload_regular(
        self, file_obj: IO[bytes], full_key: str, size: int
    ) -> None:
        """Upload using put_object (for files within the threshold)."""
        logger.debug("Uploading %s (%d bytes) via put_object", full_key, size)

        # Read entire file into memory for put_object
        # For files up to ~100MB this is acceptable
        data = file_obj.read()
        self._client.put_object(
            Bucket=self._config.bucket,
            Key=full_key,
            Body=data,
        )
        logger.debug("Upload completed: %s", full_key)

    def _upload_multipart(
        self,
        file_obj: IO[bytes],
        full_key: str,
        size: int,
        chunk_mb: int,
    ) -> None:
        """Upload using multipart upload API (for large files)."""
        chunk_size = chunk_mb * 1024 * 1024
        logger.info(
            "Uploading %s (%d bytes) via multipart (%d MB chunks)",
            full_key, size, chunk_mb,
        )

        upload_id = None
        try:
            # Initiate multipart upload
            response = self._client.create_multipart_upload(
                Bucket=self._config.bucket,
                Key=full_key,
            )
            upload_id = response["UploadId"]

            parts = []
            part_number = 1

            while True:
                chunk = file_obj.read(chunk_size)
                if not chunk:
                    break

                part_response = self._client.upload_part(
                    Bucket=self._config.bucket,
                    Key=full_key,
                    UploadId=upload_id,
                    PartNumber=part_number,
                    Body=chunk,
                )

                parts.append({
                    "PartNumber": part_number,
                    "ETag": part_response["ETag"],
                })

                logger.debug(
                    "Uploaded part %d/%d for %s",
                    part_number,
                    max(1, (size + chunk_size - 1) // chunk_size),
                    full_key,
                )
                part_number += 1

            # Complete multipart upload
            self._client.complete_multipart_upload(
                Bucket=self._config.bucket,
                Key=full_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
            logger.info("Multipart upload completed: %s (%d parts)", full_key, len(parts))

        except Exception:
            # Abort multipart upload on failure to avoid orphaned parts
            if upload_id:
                try:
                    self._client.abort_multipart_upload(
                        Bucket=self._config.bucket,
                        Key=full_key,
                        UploadId=upload_id,
                    )
                    logger.warning("Aborted multipart upload for %s", full_key)
                except Exception as abort_err:
                    logger.error(
                        "Failed to abort multipart upload for %s: %s",
                        full_key, abort_err,
                    )
            raise

    def upload_tracking(self, manifest_json: str, sftp_folder: str) -> None:
        """Upload a tracking JSON to the tracking prefix in S3.

        Args:
            manifest_json: The JSON string to upload.
            sftp_folder: The SFTP folder name (used to derive the S3 key).

        Raises:
            S3AuthError: If credentials are invalid (fatal).
            S3UploadError: If upload fails.
        """
        from migrator.manifest import _sanitize_folder_name

        folder_name = _sanitize_folder_name(sftp_folder)
        tracking_key = (
            f"{self._config.tracking_prefix.strip('/')}/{folder_name}.json"
        )

        try:
            self._client.put_object(
                Bucket=self._config.bucket,
                Key=tracking_key,
                Body=manifest_json.encode("utf-8"),
                ContentType="application/json",
            )
            logger.info(
                "Uploaded tracking to s3://%s/%s",
                self._config.bucket, tracking_key,
            )
        except (NoCredentialsError, PartialCredentialsError) as e:
            raise S3AuthError(f"AWS credentials error: {e}") from e
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code in ("403", "InvalidAccessKeyId", "SignatureDoesNotMatch"):
                raise S3AuthError(f"S3 access denied: {e}") from e
            raise S3UploadError(
                f"Failed to upload tracking for {sftp_folder}: {e}"
            ) from e
