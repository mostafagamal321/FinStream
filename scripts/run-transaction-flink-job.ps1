$ErrorActionPreference = "Stop"

Write-Host "Submitting FinStream TransactionScoringJob..." -ForegroundColor Cyan

docker exec -it finstream-flink-jobmanager flink run `
    -c com.finstream.flink.TransactionScoringJob `
    /opt/flink/usrlib/finstream-flink-jobs.jar
