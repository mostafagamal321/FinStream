param(
    [ValidateSet("train", "test")]
    [string]$Split = "train"
)

$StateFile = "state/transaction_replay_$Split.state"

if (!(Test-Path $StateFile)) {
    Write-Host "No state file found for split=$Split" -ForegroundColor Yellow
    Write-Host "Expected path: $StateFile"
    exit 0
}

$StartRow = (Get-Content -Path $StateFile -Raw).Trim()

Write-Host "Replay state" -ForegroundColor Cyan
Write-Host "Split:        $Split"
Write-Host "State file:   $StateFile"
Write-Host "Next StartRow: $StartRow" -ForegroundColor Green