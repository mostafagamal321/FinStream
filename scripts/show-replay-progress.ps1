param(
    [ValidateSet("train", "test")]
    [string]$Split = "train"
)

$ErrorActionPreference = "Stop"

$StateFile = "state/transaction_replay_$Split.state"

if ($Split -eq "train") {
    $CsvPath = "data/raw/ieee_fraud/train_transaction.csv"
} else {
    $CsvPath = "data/raw/ieee_fraud/test_transaction.csv"
}

if (!(Test-Path $CsvPath)) {
    throw "CSV file not found: $CsvPath"
}

if (!(Test-Path $StateFile)) {
    throw "State file not found: $StateFile. Run reset-replay-state.ps1 first."
}

$NextStartRow = [int](Get-Content -Path $StateFile -Raw).Trim()

Write-Host "Counting rows in $CsvPath ..." -ForegroundColor Cyan

# Counts data rows excluding header
$TotalLines = 0
Get-Content $CsvPath -ReadCount 10000 | ForEach-Object {
    $TotalLines += $_.Count
}

$TotalRows = $TotalLines - 1

$SentRows = [Math]::Min($NextStartRow, $TotalRows)
$RemainingRows = [Math]::Max($TotalRows - $SentRows, 0)

if ($TotalRows -gt 0) {
    $PercentDone = [Math]::Round(($SentRows / $TotalRows) * 100, 2)
} else {
    $PercentDone = 0
}

Write-Host ""
Write-Host "Replay progress" -ForegroundColor Green
Write-Host "Split:          $Split"
Write-Host "CSV path:       $CsvPath"
Write-Host "Total rows:     $TotalRows"
Write-Host "Next StartRow:  $NextStartRow"
Write-Host "Rows sent:      $SentRows"
Write-Host "Rows remaining: $RemainingRows"
Write-Host "Progress:       $PercentDone%"