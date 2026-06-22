"""S3 client wrapper (Functional).

Provides streaming uploads (regular and multipart) via boto3.
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
from migrator.types import S3Context


logger = logging.getLogger("migrator.s3")

# 8 MB read buffer for streaming uploads
STREAM_BUFFER_SIZE = 8 * 1024 * 1024


class S3AuthError(Exception):
    """Fatal: S3 credentials are invalid or missing."""
    pass


class S3UploadError(Exception):
    """Non-fatal: a single upload failed."""
    pass


def create_s3_context(config: S3Config) -> S3Context:
    """Initialize the boto3 S3 client and return the context.

    Raises:
        S3AuthError: If AWS credentials are missing or invalid.
    """
    try:
        client = boto3.client(
            "s3",
            region_name=config.region,
        )
        # Validate credentials with a lightweight call
        client.head_bucket(Bucket=config.bucket)
        logger.info(
            "S3 connection validated — bucket: %s, region: %s",
            config.bucket,
            config.region,
        )
        return S3Context(config=config, client=client)
    except (NoCredentialsError, PartialCredentialsError) as e:
        raise S3AuthError(
            f"AWS credentials not found or incomplete: {e}"
        ) from e
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code in ("403", "InvalidAccessKeyId", "SignatureDoesNotMatch"):
            raise S3AuthError(
                f"S3 access denied for bucket {config.bucket}: {e}"
            ) from e
        elif error_code == "404":
            raise S3AuthError(
                f"S3 bucket not found: {config.bucket}"
            ) from e
        raise S3AuthError(
            f"S3 connection error: {e}"
        ) from e


def _build_key(config: S3Config, relative_path: str) -> str:
    """Build the full S3 key from a relative path."""
    clean_path = relative_path.lstrip("/")
    if config.prefix:
        return f"{config.prefix.strip('/')}/{clean_path}"
    return clean_path


def upload_stream(
    ctx: S3Context,
    file_obj: IO[bytes],
    s3_key: str,
    size: int,
    multipart_threshold: int,
    multipart_chunk_mb: int,
) -> None:
    """Upload a file stream to S3.

    Chooses between regular put_object and multipart upload based on
    the file size.
    """
    full_key = _build_key(ctx.config, s3_key)

    try:
        if size <= multipart_threshold:
            _upload_regular(ctx, file_obj, full_key, size)
        else:
            _upload_multipart(ctx, file_obj, full_key, size, multipart_chunk_mb)
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
    ctx: S3Context, file_obj: IO[bytes], full_key: str, size: int
) -> None:
    """Upload using put_object (for files within the threshold)."""
    logger.debug("Uploading %s (%d bytes) via put_object", full_key, size)
    data = file_obj.read()
    ctx.client.put_object(
        Bucket=ctx.config.bucket,
        Key=full_key,
        Body=data,
    )
    logger.debug("Upload completed: %s", full_key)


def _upload_multipart(
    ctx: S3Context,
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
        response = ctx.client.create_multipart_upload(
            Bucket=ctx.config.bucket,
            Key=full_key,
        )
        upload_id = response["UploadId"]

        parts = []
        part_number = 1

        while True:
            chunk = file_obj.read(chunk_size)
            if not chunk:
                break

            part_response = ctx.client.upload_part(
                Bucket=ctx.config.bucket,
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

        ctx.client.complete_multipart_upload(
            Bucket=ctx.config.bucket,
            Key=full_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
        logger.info("Multipart upload completed: %s (%d parts)", full_key, len(parts))

    except Exception:
        if upload_id:
            try:
                ctx.client.abort_multipart_upload(
                    Bucket=ctx.config.bucket,
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





def delete_object(ctx: S3Context, s3_key: str) -> None:
    """Delete an object from S3."""
    full_key = _build_key(ctx.config, s3_key)
    try:
        ctx.client.delete_object(
            Bucket=ctx.config.bucket,
            Key=full_key,
        )
        logger.info("Deleted S3 object: %s", full_key)
    except (NoCredentialsError, PartialCredentialsError) as e:
        raise S3AuthError(f"AWS credentials error during deletion: {e}") from e
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code in ("403", "InvalidAccessKeyId", "SignatureDoesNotMatch"):
            raise S3AuthError(f"S3 access denied: {e}") from e
        raise S3UploadError(
            f"Failed to delete {full_key}: {e}"
        ) from e
