#!/bin/bash
set -e

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
cd "$SCRIPT_DIR"

echo "Installing requirements (user scope)..."
pip3 install --user -r requirements.txt --break-system-packages || pip3 install --user -r requirements.txt

if ! command -v gcloud >/dev/null 2>&1; then
    echo "gcloud not found. Installing Google Cloud SDK..."
    curl -sSL https://sdk.cloud.google.com | bash -s -- --disable-prompts
    source "$HOME/google-cloud-sdk/path.bash.inc"
fi

if ! gcloud auth application-default print-access-token >/dev/null 2>&1; then
    echo "No GCP credentials found. Launching login..."
    gcloud auth application-default login
fi

echo "Running content backfill..."
python3 backfill_content.py
