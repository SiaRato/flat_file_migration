# SFTP-to-S3 File Migrator

A self-contained Python tool for migrating files from an SFTP server to AWS S3.  
Designed for deployment on **air-gapped midman servers** with network access limited to SFTP and S3 only.

## Architecture

```
┌──────────────┐         ┌──────────────────┐         ┌─────────────┐
│  SFTP Server │ ◄─────► │  Midman Server   │ ◄─────► │   AWS S3    │
│  (source)    │  SFTP   │  (runs migrator) │  HTTPS  │  (target)   │
└──────────────┘         └──────────────────┘         └─────────────┘
```

## Features

- **Infinite Scale Database Tracking** — backed by an ultra-fast local SQLite database instead of in-memory JSON to support tens of millions of files.
- **Daemon Scheduler** — running `./run.sh` launches the persistent Daemon by default. It reads standard cron schedules directly from `folders.txt` and manages its own internal triggers via a SQLite scheduling database.
- **Strict Mirror Sync** — automatically deletes orphaned S3 files that were removed from the SFTP server, keeping the destination perfectly in sync.
- **Streaming SFTP Generator** — pulls file metadata over the network one-by-one, keeping memory usage flat regardless of folder size.
- **Stream-based transfer** — files are streamed directly from SFTP to S3 without touching local disk.
- **Concurrent transfers** — configurable thread pool for parallel file uploads within each folder.
- **Multipart uploads** — automatic for files larger than a configurable threshold (default: 100 MB).
- **Smart change detection** — skips files that haven't changed (size + mtime comparison).
- **Stateless Resilience** — tracking history is downloaded from S3 at the start, making the midman server completely disposable.

## Prerequisites

- Python 3.10+ on the midman server
- SFTP server credentials (password-based authentication)
- AWS credentials with `s3:PutObject`, `s3:CreateMultipartUpload`, `s3:DeleteObject`, etc. on the target bucket

## Building the Bundle

Run on a machine **with internet access**:

```bash
chmod +x build.sh
./build.sh
```

This creates `dist/migrator-bundle.tar.gz` containing all source code and vendored dependencies.

## Deploying to Midman Server

```bash
# Copy bundle to midman server
scp dist/migrator-bundle.tar.gz user@midman-server:/opt/

# Extract on midman server
ssh user@midman-server
cd /opt
tar -xzf migrator-bundle.tar.gz
cd migrator

# Create your configuration
cp config.example.yaml config.yaml    # edit with your settings
cp folders.example.txt folders.txt    # edit with your SFTP folder paths
```

## Configuration

### `config.yaml`

```yaml
sftp:
  host: "sftp.example.com"
  port: 22
  username: "myuser"
  # password from env: SFTP_PASSWORD

s3:
  bucket: "my-migration-bucket"
  prefix: ""
  region: "us-east-1"
  tracking_prefix: "migration-tracking"

options:
  max_workers: 4
  multipart_threshold_mb: 100
  multipart_chunk_mb: 16
  max_retries: 3
  retry_backoff_base: 2
  consecutive_failure_threshold: 5
  log_file: "logs/migrator.log"
```

One SFTP folder path per line. You can optionally prefix lines with a standard 5-part crontab expression to schedule jobs:

```
# Scheduled jobs
0 2 * * * /data/exports/2024
*/15 * * * * /data/reports

# One-time jobs (will run exactly once per daemon boot)
/archive/logs
```

### Environment Variables

| Variable | Required | Description |
|---|---|---|
| `SFTP_PASSWORD` | Yes | SFTP password |
| `AWS_ACCESS_KEY_ID` | Yes | AWS access key |
| `AWS_SECRET_ACCESS_KEY` | Yes | AWS secret key |
| `AWS_SESSION_TOKEN` | No | For temporary credentials |
| `AWS_DEFAULT_REGION` | No | Fallback if not in config |

## Running

```bash
# Set credentials
export SFTP_PASSWORD=xxx
export AWS_ACCESS_KEY_ID=xxx
export AWS_SECRET_ACCESS_KEY=xxx

# Preview (recommended first run)
./run.sh --dry-run

# Run the persistent daemon scheduler (Default mode)
./run.sh

# Override folders.txt and run a single specific folder manually
./run.sh --folder /data/reports

# Verbose logging
./run.sh --log-level DEBUG
```

## How It Works

### Per-Folder Lifecycle

1. **Load tracking** — downloads the historical SQLite tracking database from S3.
2. **Streaming Discovery** — uses a generator to stream files from the SFTP folder one by one.
3. **Filter** — instantly queries the SQLite database to skip files already migrated (size + mtime match).
4. **Upload `in_progress`** — pushes tracking DB to S3 to signal migration has started.
5. **Transfer** — streams files from SFTP to S3 using batched concurrent workers.
6. **Mirror Pruning** — identifies files missing from SFTP and gracefully deletes them from S3.
7. **Finalize**:
   - ✓ All files succeeded → upload `completed` tracking DB.
   - ✗ Some files failed → upload `failed` tracking DB → keep local tracking for next run.

### Error Handling

| Error Type | Examples | Behavior |
|---|---|---|
| **Fatal** | SFTP connection refused, S3 auth expired, network unreachable | Halt program immediately |
| **Consecutive threshold** | 5+ files fail in a row | Halt program (systemic issue) |
| **Per-file** | File permission denied, transient timeout | Retry with exponential backoff, then continue |

### Tracking Status on S3

Check `s3://bucket/migration-tracking/*.db` to monitor progress. You can download these SQLite `.db` files and run queries locally to view deep statistics.

## Deployment Structure

```
/opt/migrator/
├── run.sh              ← launcher
├── config.yaml         ← your settings (no secrets)
├── folders.txt         ← SFTP folders to migrate
├── src/migrator/       ← source code
├── vendor/             ← vendored Python packages
├── tracking/           ← ephemeral tracking SQLite DBs
└── logs/               ← log files
```

## Troubleshooting

**"SFTP_PASSWORD environment variable is not set"**  
→ Set `export SFTP_PASSWORD=xxx` before running.

**"S3 authentication failed"**  
→ Check `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` are set and valid.

**"N consecutive failures — systemic issue detected"**  
→ Check SFTP server status and network connectivity. Re-run after fixing.

**"Skipping /data/reports — already covered by parent /data"**  
→ Normal behavior. Nested folders are deduplicated to avoid double-migration.
