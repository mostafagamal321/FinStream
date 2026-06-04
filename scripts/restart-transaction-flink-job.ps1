$ErrorActionPreference = "Stop"

Write-Host "Checking existing Flink jobs..." -ForegroundColor Cyan

$jobsOutput = docker exec finstream-flink-jobmanager flink list 2>&1

$jobIds = $jobsOutput |
    Select-String -Pattern "([a-f0-9]{32})" |
    ForEach-Object { $_.Matches[0].Value } |
    Select-Object -Unique

foreach ($jobId in $jobIds) {
    Write-Host "Cancelling Flink job: $jobId" -ForegroundColor Yellow
    docker exec finstream-flink-jobmanager flink cancel $jobId
}

Write-Host "Starting Flink containers..." -ForegroundColor Cyan

docker compose --profile streaming up -d flink-jobmanager flink-taskmanager

Start-Sleep -Seconds 8

Write-Host "Submitting TransactionScoringJob..." -ForegroundColor Cyan

docker exec -it finstream-flink-jobmanager flink run `
    -c com.finstream.flink.TransactionScoringJob `
    /opt/flink/usrlib/finstream-flink-jobs.jar
