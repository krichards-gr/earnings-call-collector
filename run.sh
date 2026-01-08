#!/bin/bash
set -e

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
cd "$SCRIPT_DIR"

# Install dependencies if needed
python3 -m pip install --user -r requirements.txt 2>/dev/null || python3 -m pip install --user --break-system-packages -r requirements.txt

# Run the script
python3 sql_get.py "$@"
