#!/bin/bash
# Build script for the SFTP-to-S3 Migrator.
# Run this on a machine WITH internet access to create a self-contained
# deployment bundle for the midman server.
#
# Output: dist/migrator-bundle.tar.gz
#
# Usage:
#   ./build.sh
#   scp dist/migrator-bundle.tar.gz user@midman-server:/opt/

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DIST_DIR="${SCRIPT_DIR}/dist"
BUNDLE_DIR="${DIST_DIR}/migrator"

echo "=== SFTP-to-S3 Migrator — Build Script ==="
echo ""

# Clean previous build
if [ -d "${DIST_DIR}" ]; then
    echo "Cleaning previous build..."
    rm -rf "${DIST_DIR}"
fi

# Create bundle directory structure
echo "Creating bundle directory..."
mkdir -p "${BUNDLE_DIR}/vendor"
mkdir -p "${BUNDLE_DIR}/tracking"
mkdir -p "${BUNDLE_DIR}/logs"

# Install dependencies into vendor/
echo "Installing dependencies into vendor/..."
pip install \
    --target "${BUNDLE_DIR}/vendor" \
    --no-cache-dir \
    --quiet \
    -r "${SCRIPT_DIR}/requirements.txt"

echo "Dependencies installed."

# Copy source code
echo "Copying source code..."
cp -r "${SCRIPT_DIR}/src" "${BUNDLE_DIR}/src"

# Copy launcher
cp "${SCRIPT_DIR}/run.sh" "${BUNDLE_DIR}/run.sh"
chmod +x "${BUNDLE_DIR}/run.sh"

# Copy config templates
cp "${SCRIPT_DIR}/config.example.yaml" "${BUNDLE_DIR}/config.example.yaml"
cp "${SCRIPT_DIR}/folders.example.yaml" "${BUNDLE_DIR}/folders.example.yaml"

# Copy README
if [ -f "${SCRIPT_DIR}/README.md" ]; then
    cp "${SCRIPT_DIR}/README.md" "${BUNDLE_DIR}/README.md"
fi

# Create tarball
echo "Creating tarball..."
cd "${DIST_DIR}"
tar -czf migrator-bundle.tar.gz migrator/
cd "${SCRIPT_DIR}"

BUNDLE_SIZE=$(du -sh "${DIST_DIR}/migrator-bundle.tar.gz" | cut -f1)

echo ""
echo "=== Build Complete ==="
echo "Bundle: ${DIST_DIR}/migrator-bundle.tar.gz (${BUNDLE_SIZE})"
echo ""
echo "Deploy to midman server:"
echo "  scp ${DIST_DIR}/migrator-bundle.tar.gz user@midman-server:/opt/"
echo "  ssh user@midman-server 'cd /opt && tar -xzf migrator-bundle.tar.gz'"
echo ""
echo "Then on the midman server:"
echo "  cd /opt/migrator"
echo "  cp config.example.yaml config.yaml   # edit with your settings"
echo "  cp folders.example.yaml folders.yaml    # edit with your folders"
echo "  export SFTP_PASSWORD=xxx"
echo "  export AWS_ACCESS_KEY_ID=xxx"
echo "  export AWS_SECRET_ACCESS_KEY=xxx"
echo "  ./run.sh --dry-run                    # preview first"
echo "  ./run.sh                              # run migration"
