$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$wslPath = $scriptDir -replace '\\', '/' -replace '^C:', '/mnt/c'
Write-Host "Running backfill in WSL..."
wsl bash -c "cd '$wslPath' && bash backfill.sh" 2>&1
if ($LASTEXITCODE -ne 0) { Write-Error "backfill.sh exited with code $LASTEXITCODE" }
