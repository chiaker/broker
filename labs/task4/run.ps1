$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

Write-Host "[1/3] docker compose up -d ..."
docker compose up -d --build

Write-Host "[2/3] waiting for MySQL ..."
$deadline = (Get-Date).AddSeconds(120)
while ((Get-Date) -lt $deadline) {
    $h = docker inspect --format '{{.State.Health.Status}}' task4_mysql 2>$null
    if ($h -eq "healthy") { Write-Host "MySQL healthy"; break }
    Start-Sleep -Seconds 2
}

Write-Host "[3/3] installing python deps and running demo.py ..."
python -m pip install -q -r requirements.txt
python demo.py
