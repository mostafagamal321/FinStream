param(
    [int]$RecentMinutes = 10,
    [int]$Limit = 10
)

$ErrorActionPreference = "Continue"

Write-Host ""
Write-Host "FINSTREAM END-TO-END HEALTH CHECK" -ForegroundColor Cyan
Write-Host "================================="
Write-Host ""

Write-Host "1) Docker containers" -ForegroundColor Yellow
docker ps --format "table {{.Names}}`t{{.Status}}`t{{.Image}}" | findstr /I "finstream-kafka finstream-schema-registry finstream-flink-jobmanager finstream-flink-taskmanager finstream-clickhouse finstream-redis finstream-spark-master finstream-spark-worker"
Write-Host ""

Write-Host "2) Flink jobs" -ForegroundColor Yellow
docker exec finstream-flink-jobmanager flink list
Write-Host ""

Write-Host "3) Kafka transaction lag" -ForegroundColor Yellow
.\scripts\check-transaction-lag.ps1
Write-Host ""

Write-Host "4) Latest fraud scores from ClickHouse" -ForegroundColor Yellow
.\scripts\check-fraud-scores.ps1 -Limit $Limit
Write-Host ""

Write-Host "5) Recent decision distribution" -ForegroundColor Yellow
docker exec finstream-clickhouse clickhouse-client --user finstream --password finstream123 --query "SELECT decision, risk_level, count() FROM finstream.fraud_scores_rt WHERE inserted_at >= now() - INTERVAL $RecentMinutes MINUTE GROUP BY decision, risk_level ORDER BY decision, risk_level;"
Write-Host ""

Write-Host "6) Redis online feature metadata" -ForegroundColor Yellow
Write-Host "last_online_update_at:"
docker exec finstream-redis redis-cli GET feature_store:last_online_update_at
Write-Host "last_online_transaction_id:"
docker exec finstream-redis redis-cli GET feature_store:last_online_transaction_id
Write-Host ""

Write-Host "7) Redis command stats" -ForegroundColor Yellow
docker exec finstream-redis redis-cli INFO commandstats | findstr /I "cmdstat_get cmdstat_set cmdstat_expire cmdstat_setex"
Write-Host ""

Write-Host "8) Scorecard model metadata from S3" -ForegroundColor Yellow
.\scripts\check-scorecard-thresholds.ps1
Write-Host ""

Write-Host "Health check finished." -ForegroundColor Green
