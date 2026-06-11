param(
    [ValidateSet("train", "test")]
    [string]$Split = "train",

    [int]$StartRow = 0
)

$ErrorActionPreference = "Stop"

$StateDir = "state"
$StateFile = Join-Path $StateDir "transaction_replay_$Split.state"

if (!(Test-Path $StateDir)) {
    New-Item -ItemType Directory -Path $StateDir | Out-Null
}

Set-Content -Path $StateFile -Value $StartRow

Write-Host "Replay state reset." -ForegroundColor Green
Write-Host "Split:         $Split"
Write-Host "State file:    $StateFile"
Write-Host "Next StartRow: $StartRow"