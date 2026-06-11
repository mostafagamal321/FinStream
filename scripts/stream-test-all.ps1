param(
    [int]$Rate = 1,
    [int]$BatchSize = 100,
    [int]$ChunkSize = 10000,
    [int]$DefaultStartRow = 0
)

$ErrorActionPreference = "Stop"

$Split = "test"
$CsvPath = "data/raw/ieee_fraud/test_transaction.csv"

$StateDir = "state"
$StateFile = Join-Path $StateDir "transaction_replay_test_stream.state"

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

Write-Host "Counting test CSV rows..." -ForegroundColor Cyan

$TotalLines = 0
Get-Content $CsvPath -ReadCount 10000 | ForEach-Object {
    $TotalLines += $_.Count
}

$TotalRows = $TotalLines - 1

Write-Host ""
Write-Host "Starting continuous test stream" -ForegroundColor Green
Write-Host "CSV path:    $CsvPath"
Write-Host "Total rows:  $TotalRows"
Write-Host "Rate:        $Rate record/sec"
Write-Host "Batch size:  $BatchSize"
Write-Host "State file:  $StateFile"
Write-Host ""

while ($true) {
    $StartRow = [int](Get-Content -Path $StateFile -Raw).Trim()

    if ($StartRow -ge $TotalRows) {
        Write-Host "All test rows have been streamed." -ForegroundColor Green
        Write-Host "Total rows:    $TotalRows"
        Write-Host "Final row:     $StartRow"
        break
    }

    $RemainingRows = $TotalRows - $StartRow
    $EffectiveLimit = [Math]::Min($BatchSize, $RemainingRows)
    $NextStartRow = $StartRow + $EffectiveLimit

    Write-Host ""
    Write-Host "Sending test batch..." -ForegroundColor Cyan
    Write-Host "StartRow:       $StartRow"
    Write-Host "Limit:          $EffectiveLimit"
    Write-Host "NextStartRow:   $NextStartRow"
    Write-Host "Remaining:      $($TotalRows - $NextStartRow)"
    Write-Host ""

    .\scripts\send-transactions.ps1 `
        -Split test `
        -StartRow $StartRow `
        -Limit $EffectiveLimit `
        -Rate $Rate `
        -ChunkSize $ChunkSize

    if ($LASTEXITCODE -ne 0) {
        throw "send-transactions.ps1 failed. State was not advanced."
    }

    Set-Content -Path $StateFile -Value $NextStartRow

    Write-Host "State updated: NextStartRow=$NextStartRow" -ForegroundColor Green
}