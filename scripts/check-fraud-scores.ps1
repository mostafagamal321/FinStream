param(
    [int]$Limit = 20
)

$ErrorActionPreference = "Stop"

docker exec -i finstream-clickhouse clickhouse-client `
    --user finstream `
    --password finstream123 `
    --query "SELECT inserted_at, transaction_id, rule_score, ml_score, fraud_score, scoring_method, risk_level, decision, reason_codes FROM finstream.fraud_scores_rt WHERE inserted_at >= now() - INTERVAL 10 MINUTE ORDER BY inserted_at DESC LIMIT $Limit;"
