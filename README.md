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

- **Database Tracking** — backed by local SQLite database to support large scale migration.
- **Daemon Scheduler** — running `./run.sh` launches the persistent scheduler. It reads standard cron schedules directly from `folders.yaml` and manages its own internal triggers via a SQLite scheduling database.
- **Strict Mirror Sync** — automatically deletes files on S3 that were removed from the SFTP server, keeping the destination perfectly in sync.
- **Streaming SFTP Generator & Predictable Sync** — pulls file metadata from SFTP over the network one-by-one, or uses template-based prediction to avoid slow `ls` operations entirely, keeping memory usage flat regardless of folder size.
- **Stream-based transfer** — files are streamed directly from SFTP to S3 without touching local disk.
- **Concurrent transfers** — configurable thread pool for parallel file uploads within each folder.
- **Multipart uploads** — automatic for files larger than a configurable threshold (default: 100 MB).
- **Smart change detection** — skips files that haven't changed (size + mtime comparison).
- **Persistent Local Tracking** — tracking history is stored in a local SQLite database, avoiding reliance on remote state and providing extremely fast state recovery.

## How It Scales

This migrator is specifically engineered to handle extreme scaling scenarios without crashing:

### 1. Handling a Large Number of Files (Millions of files)
Standard scripts often crash on massive directories due to Out of Memory (OOM) errors. This migrator avoids that by using:
- **SFTP Generators**: Instead of loading the entire list of files into a giant array at once, it uses a Python generator to pull file names from the SFTP server one by one. 
- **SQLite Tracking**: It queries a highly optimized local SQLite database to check if a file has already been transferred, instead of keeping a massive dictionary in memory. 
*Result: Memory usage stays completely flat whether you are transferring 10 files or 10 million files.*

### 2. Handling Massive Individual Files (e.g., 50GB+ files)
Standard scripts will often run out of hard drive space or timeout on huge files. This migrator handles it by:
- **Zero Disk Usage**: Data is streamed directly from the SFTP server straight into AWS S3. The file payload is never saved to the local server's hard drive.
- **Multipart Chunking**: For any file larger than 100MB, the migrator automatically switches to S3 Multipart Uploads. It reads the file in tiny 16MB chunks, holds just that one chunk in memory, uploads it to S3, and repeats until finished.

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
cp folders.example.yaml folders.yaml   # edit with your SFTP folder and target paths
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

options:
  max_workers: 4
  multipart_threshold_mb: 100
  multipart_chunk_mb: 16
  max_retries: 3
  retry_backoff_base: 2
  consecutive_failure_threshold: 5
  log_file: "logs/migrator.log"
```

The `folders.yaml` configuration defines the source folders, their crontab schedule, and their target S3 mapping:

```yaml
folders:
  # Scheduled jobs
  - source: /data/exports/2024
    cron: "0 2 * * *"
    target: my_s3_prefix/2024

  - source: /data/reports
    cron: "*/15 * * * *"
    target: reports/daily

  # One-time jobs (will run exactly once per daemon boot)
  - source: /archive/logs
    target: archive/logs

  # Append-Only Predictable Sync (For massive flat directories)
  - source: /data/flat_history
    target: flat_history
    cron: "0 * * * *"
    mirror_deletions: false
    filename_template: "report_{date:%Y%m%d}_{seq:1-5000:04d}.csv"
    schedule_lookback_days: 15
    initial_lookback_days: 7300
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

# Override folders.yaml and run a single specific folder manually
./run.sh --folder /data/reports --target reports/daily

# Verbose logging
./run.sh --log-level DEBUG
```

## Running as a System Service (systemd)

To ensure the migrator runs continuously in the background and starts automatically on system reboot, you can configure it as a `systemd` service on Linux.

1. Create an environment file at `/opt/migrator/.env` with your credentials:
```bash
SFTP_PASSWORD=your_sftp_password
AWS_ACCESS_KEY_ID=your_aws_key
AWS_SECRET_ACCESS_KEY=your_aws_secret
```
*(Run `chmod 600 /opt/migrator/.env` to protect your secrets.)*

2. Create a service file at `/etc/systemd/system/migrator.service` (requires `sudo`):
```ini
[Unit]
Description=SFTP-to-S3 Migrator Daemon
After=network.target

[Service]
Type=simple
User=your_user  # Change this to your deployment username
WorkingDirectory=/opt/migrator
ExecStart=/opt/migrator/run.sh
Restart=always
RestartSec=10
EnvironmentFile=/opt/migrator/.env

[Install]
WantedBy=multi-user.target
```

3. Enable and start the service:
```bash
sudo systemctl daemon-reload
sudo systemctl enable migrator.service
sudo systemctl start migrator.service
```

You can view the logs anytime using `journalctl -u migrator.service -f` or by checking the local `logs/migrator.log` file.

## How It Works

### Per-Folder Lifecycle

1. **Load tracking** — initializes or loads the local SQLite tracking database.
2. **Streaming Discovery** — uses a generator to stream files from the SFTP folder one by one.
3. **Filter** — instantly queries the SQLite database to skip files already migrated (size + mtime match).
4. **Transfer** — streams files from SFTP to S3 using batched concurrent workers.
5. **Mirror Pruning** — identifies files missing from SFTP and gracefully deletes them from S3.
6. **Finalize**: Updates the local tracking database to `completed` or `failed`.

### Error Handling

| Error Type | Examples | Behavior |
|---|---|---|
| **Fatal** | SFTP connection refused, S3 auth expired, network unreachable | Halt program immediately |
| **Consecutive threshold** | 5+ files fail in a row | Halt program (systemic issue) |
| **Per-file** | File permission denied, transient timeout | Retry with exponential backoff, then continue |

### Tracking Status

Tracking is managed entirely locally in the `tracking/` directory. You can query the local SQLite `.db` files to view deep statistics.

## Deployment Structure

```
/opt/migrator/
├── run.sh              ← launcher
├── config.yaml         ← your settings (no secrets)
├── folders.yaml        ← SFTP folders and S3 targets
├── src/migrator/       ← source code
├── vendor/             ← vendored Python packages
├── tracking/           ← persistent local tracking SQLite DBs
└── logs/               ← log files
```

## Codebase Structure for Developers

If you are a junior developer or simply looking to understand, modify, or extend the logic of the codebase, here is a breakdown of the core modules located in `src/migrator/`:

- `__main__.py`: The entry point. Parses CLI arguments (`--folder`, `--dry-run`) and launches either a single directory run or the persistent daemon scheduler.
- `scheduler.py`: The daemon scheduler. Uses a local SQLite database to manage cron jobs and one-time tasks, triggering folder transfers when scheduled.
- `transfer.py`: The core orchestrator. Contains the business logic for concurrency, consuming the SFTP generator, batching uploads, and deleting orphaned files on S3.
- `s3_client.py`: AWS Boto3 wrapper. Handles multipart chunking and credentials validation.
- `sftp_client.py`: Paramiko wrapper. Connects to SFTP and recursively walks directories using generators to ensure memory usage remains flat regardless of folder depth.
- `manifest.py`: SQLite wrapper. Tracks local file states (`seen`, `size`, `mtime`) to ensure we only transfer new or changed files. Replaces in-memory JSON to guarantee infinite horizontal scale.
- `config.py`: Parses `config.yaml`, validates crontab expressions, and securely loads environment variables.

**Design Philosophy**: The codebase favors functional programming paradigms (pure functions, callbacks) over stateful class OOP. State is explicitly passed around via context dataclasses (`S3Context`, `SFTPContext`) to avoid mutating class instance variables.

## Troubleshooting

**"SFTP_PASSWORD environment variable is not set"**  
→ Set `export SFTP_PASSWORD=xxx` before running.

**"S3 authentication failed"**  
→ Check `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` are set and valid.

**"N consecutive failures — systemic issue detected"**  
→ Check SFTP server status and network connectivity. Re-run after fixing.

**"Overlapping folders detected..."**  
→ Normal behavior. Nested and overlapping folders trigger a fatal error to avoid double-migration. Please fix your `folders.yaml`.
