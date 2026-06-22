# Technical Specification: SFTP-to-S3 File Migrator

## 1. Introduction
**Purpose**: A robust, self-contained Python tool designed to migrate files from an SFTP server to AWS S3. It is built specifically for "midman" servers operating in constrained or air-gapped environments, ensuring files are streamed directly to S3 without utilizing local disk storage for payloads. The tool supports continuous incremental mirroring to accommodate millions of files at a 10TB scale.

## 2. Architecture & Environment
- **Source**: SFTP Server (requires password authentication).
- **Midman**: A machine running Python 3.10+ where the migrator script executes. Requires outbound port 22 (or custom) to the SFTP server, and outbound HTTPS (443) to AWS S3.
- **Target**: AWS S3 Bucket.

## 3. Core Logic & Lifecycle
### 3.1 Folder Processing Workflow
The application operates as a persistent daemon scheduler across a list of directories defined in `folders.yaml`. These directories can include optional standard 5-part crontab expressions.

1. **State Bootstrapping**: The tool initializes or loads a local SQLite tracking database (`tracking/{folder}.db`).
2. **Daemon Scheduling**: A central `scheduler.db` manages the exact `next_run` timestamp for each folder. Folders without a cron expression are run exactly once per daemon boot to check for missed updates.
3. **SFTP Discovery (Streaming)**: Connects to the SFTP server and uses a Python generator to yield files one-by-one, ensuring constant minimal memory usage regardless of folder depth.
4. **Change Detection**: Batches of files are instantly compared against the local SQLite database. Files with matching `size` and `mtime` (Modification Time) are skipped and marked as "seen".
5. **Concurrent Transfer**: Batches of queued files are passed to a `ThreadPoolExecutor` to stream multiple files in parallel to S3.
6. **Mirror Deletions**: After the SFTP generator finishes, any file in the SQLite database that was not "seen" during discovery is identified as an orphaned file. These orphans are deleted from S3 and purged from the database.
7. **Finalize**: Updates the database status to `completed` or `failed`.

### 3.2 File Transfer Mechanics (Streaming)
File transfers do not write to the midman's disk. 
- **Standard Uploads**: Files smaller than `multipart_threshold_mb` (Default: 100MB) are streamed directly into memory and uploaded via a standard Boto3 `put_object` call.
- **Multipart Uploads**: Files larger than the threshold utilize the S3 Multipart Upload API. The stream is read in chunks defined by `multipart_chunk_mb` (Default: 16MB) and uploaded part by part.
- **Thread Safety**: Paramiko SFTP connections are inherently not thread-safe. To resolve this, each worker thread spawns its own distinct SFTP connection while sharing a thread-safe Boto3 client.

## 4. State Management (SQLite Database)
The system's tracking state is managed through a local SQLite database, replacing in-memory JSON to guarantee infinite horizontal scale. Memory footprint is flat, enabling the processing of millions of files.

**Database Optimizations:**
- `path` is the Primary Key, triggering automatic B-Tree indexing for microsecond lookups.
- `seen` and `status` utilize a composite index to instantly find deleted orphaned files without table scans.
- `PRAGMA journal_mode=WAL` is active to allow concurrent threads to write without locking.

**Folder-Level Local Statuses:**
- `in_progress`: Actively migrating. If a script halts unexpectedly, the status remains here, indicating a crash.
- `completed`: 100% of files within the folder migrated successfully.
- `failed`: Completed the pass, but one or more files encountered errors.

## 5. Error Handling & Resiliency
- **Transient Errors (Per-File)**: Network blips or read timeouts are caught and retried using an exponential backoff formula, capped at `max_retries`.
- **Systemic Failures**: If the system detects `consecutive_failure_threshold` sequential file failures, it assumes a systemic issue (e.g., the SFTP server went down mid-transfer) and halts the entire program to prevent unbounded looping.
- **Fatal Errors**: Complete S3 auth failures, inability to connect to the SFTP server initially, or terminal connection drops halt the process immediately.

## 6. Project Module Breakdown
Located in `src/migrator/`:
- `__main__.py`: CLI argument parsing (supports `--folder`, `--dry-run`), and entry point execution. Defaults to launching the daemon scheduler.
- `scheduler.py`: SQLite-backed daemon scheduler for managing cron jobs and one-time tasks.
- `transfer.py`: Orchestrator holding the core business logic (concurrency, batched generator consumption, mirror deletions).
- `s3_client.py`: AWS Boto3 abstraction handling multipart chunking and auth validation.
- `sftp_client.py`: Paramiko abstraction handling recursive directory walks via generators.
- `manifest.py`: SQLite wrapper for tracking local file states and executing queries.
- `config.py`: YAML parsing, crontab expression validation, and environment variable hydration.

## 7. Configuration Specifications
- `config.yaml`: Contains connection endpoints, bucket details, and operational thresholds (`max_workers`, `multipart_chunk_mb`).
- `folders.yaml`: A YAML configuration file mapping source directories to S3 targets, with optional crontab prefixes.
- Environment Variables: `SFTP_PASSWORD`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` are strictly injected via environment for security.

## 8. Extension Points & Design Rules for Future Sessions
If modifying the codebase, refer to these targets and rules:
- **Design Rule (Functional)**: Prefer functional programming paradigms and callback functions over stateful class OOP. Use explicit context dataclasses (e.g. `S3Context`, `SFTPContext`) to pass state instead of mutating class instance variables.
- **Adding new S3 Storage Classes**: Modify `s3_client.py`'s `put_object` and `create_multipart_upload` calls to accept `StorageClass` arguments (e.g., `STANDARD_IA`).
- **Changing SFTP Auth Mechanisms**: Modify `sftp_client.py` and `config.py` to support SSH Key files instead of passwords.
- **Advanced Filtering**: Modify `transfer.py` Step 3 to include regex matching or date-based filtering before adding a file to the `to_transfer` queue.
