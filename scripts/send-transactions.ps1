param(
    [ValidateSet("train", "test")]
    [string]$Split = "test",

    [int]$Limit = 20,

    [int]$Rate = 10
)

$ErrorActionPreference = "Stop"

$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
$env:SCHEMA_REGISTRY_URL = "http://localhost:8081"

Write-Host "Sending $Limit transactions from split=$Split at rate=$Rate events/sec..." -ForegroundColor Cyan

python -m src.producers.transaction_replay_producer `
    --split $Split `
    --limit $Limit `
    --rate $Rate
