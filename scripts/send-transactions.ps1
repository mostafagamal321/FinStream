param(
    [ValidateSet("train", "test")]
    [string]$Split = "test",

    [int]$StartRow = 0,

    [int]$Limit = 20,

    [int]$Rate = 10,

    [int]$ChunkSize = 10000
)

$ErrorActionPreference = "Stop"

$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
$env:BOOTSTRAP_SERVERS = "localhost:29092"
$env:SCHEMA_REGISTRY_URL = "http://localhost:8081"

# Important: Flink consumes from transactions_raw, not transaction_event
$env:TOPIC_TRANSACTIONS_RAW = "transactions_raw"

# Use the real schema path in your repo
$env:TRANSACTION_SCHEMA_PATH = "schemas/transactions/transaction_event.avsc"

Write-Host "Sending transactions..." -ForegroundColor Cyan
Write-Host "Split:     $Split"
Write-Host "StartRow:  $StartRow"
Write-Host "Limit:     $Limit"
Write-Host "Rate:      $Rate events/sec"
Write-Host "ChunkSize: $ChunkSize"
Write-Host "Topic:     $env:TOPIC_TRANSACTIONS_RAW"
Write-Host "Schema:    $env:TRANSACTION_SCHEMA_PATH"

python -m src.producers.transaction_replay_producer `
    --split $Split `
    --start-row $StartRow `
    --limit $Limit `
    --rate $Rate `
    --chunk-size $ChunkSize

if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}