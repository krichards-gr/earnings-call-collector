$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Write-Host "Executing script in WSL..."
wsl bash setup_and_run.sh
