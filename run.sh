#!/bin/bash
# Launcher script for the SFTP-to-S3 Migrator.
# Run this on the midman server after deploying the bundle.
#
# Usage:
#   export SFTP_PASSWORD=xxx
#   export AWS_ACCESS_KEY_ID=xxx
#   export AWS_SECRET_ACCESS_KEY=xxx
#   ./run.sh                          # Starts the daemon scheduler (default)
#   ./run.sh --folder /path           # Run manually on a single folder and exit
#   ./run.sh --dry-run                # Preview mode
#   ./run.sh --log-level DEBUG        # Verbose logging

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

export PYTHONPATH="${SCRIPT_DIR}/vendor:${SCRIPT_DIR}/src${PYTHONPATH:+:${PYTHONPATH}}"

exec python3 -m migrator \
    --config "${SCRIPT_DIR}/config.yaml" \
    --folders "${SCRIPT_DIR}/folders.txt" \
    "$@"
