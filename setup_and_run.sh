#!/bin/bash
set -e

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
cd "$SCRIPT_DIR"

# Clean up broken venv if it exists
if [ -d "venv_wsl" ]; then
    rm -rf venv_wsl
fi

echo "Installing requirements (user scope)..."
# Use --break-system-packages if on a managed environment (like newer Ubuntu/Debian) where pip is restricted
# OR just try standard install first. Newer ubuntu requires --break-system-packages for user installs outside venv often?
# actually, let's just try pip install --user.

pip3 install --user defeatbeta-api --break-system-packages || pip3 install --user defeatbeta-api

echo "Running SQL retrieval script..."
python3 sql_get.py "$@"
