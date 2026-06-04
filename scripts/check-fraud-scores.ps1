param(
    [int]$Limit = 20
)

$ErrorActionPreference = "Stop"

docker exec -it finstream-clickhouse clickhouse-client `
    --user finstream `
    --password finstream123 `
    --query "SELECT inserted_at, transaction_id, rule_score, ml_score, fraud_score, scoring_method, risk_level, decision, reason_codes FROM finstream.fraud_scores_rt ORDER BY inserted_at DESC LIMIT $Limit;"
