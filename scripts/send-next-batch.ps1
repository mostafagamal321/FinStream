param(
    [ValidateSet("train", "test")]
    [string]$Split = "train",

    [int]$Limit = 50000,

    [int]$Rate = 100,

    [int]$ChunkSize = 10000,

    [int]$DefaultStartRow = 0
)

$ErrorActionPreference = "Stop"

$StateDir = "state"
$StateFile = Join-Path $StateDir "transaction_replay_$Split.state"

if ($Split -eq "train") {
    $CsvPath = "data/raw/ieee_fraud/train_transaction.csv"
} else {
    $CsvPath = "data/raw/ieee_fraud/test_transaction.csv"
}

if (!(Test-Path $CsvPath)) {
    throw "CSV file not found: $CsvPath"
}

if (!(Test-Path $StateDir)) {
    New-Item -ItemType Directory -Path $StateDir | Out-Null
}

if (!(Test-Path $StateFile)) {
    Write-Host "State file not found. Creating it with StartRow=$DefaultStartRow" -ForegroundColor Yellow
    Set-Content -Path $StateFile -Value $DefaultStartRow
}

$StartRow = [int](Get-Content -Path $StateFile -Raw).Trim()

Write-Host "Counting total CSV rows..." -ForegroundColor Cyan
$TotalLines = 0
Get-Content $CsvPath -ReadCount 10000 | ForEach-Object {
    $TotalLines += $_.Count
}
$TotalRows = $TotalLines - 1

if ($StartRow -ge $TotalRows) {
    Write-Host "All rows already sent for split=$Split." -ForegroundColor Green
    Write-Host "Total rows:    $TotalRows"
    Write-Host "Next StartRow: $StartRow"
    exit 0
}

$RemainingRows = $TotalRows - $StartRow
$EffectiveLimit = [Math]::Min($Limit, $RemainingRows)
$NextStartRow = $StartRow + $EffectiveLimit

Write-Host "Sending next transaction batch..." -ForegroundColor Cyan
Write-Host "Split:          $Split"
Write-Host "CSV path:       $CsvPath"
Write-Host "Total rows:     $TotalRows"
Write-Host "StartRow:       $StartRow"
Write-Host "Limit asked:    $Limit"
Write-Host "Limit actual:   $EffectiveLimit"
Write-Host "Next StartRow:  $NextStartRow"
Write-Host "Rows remaining after this batch: $($TotalRows - $NextStartRow)"
Write-Host "Rate:           $Rate events/sec"
Write-Host "ChunkSize:      $ChunkSize"

.\scripts\send-transactions.ps1 `
    -Split $Split `
    -StartRow $StartRow `
    -Limit $EffectiveLimit `
    -Rate $Rate `
    -ChunkSize $ChunkSize

Set-Content -Path $StateFile -Value $NextStartRow

Write-Host "Batch sent successfully." -ForegroundColor Green
Write-Host "Updated state file: $StateFile"
Write-Host "Next StartRow: $NextStartRow" -ForegroundColor Green