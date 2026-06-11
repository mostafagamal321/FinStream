$ErrorActionPreference = "Stop"

docker run --rm `
    --dns 8.8.8.8 `
    --env-file .env `
    amazon/aws-cli:2.15.57 `
    s3 cp s3://finstream-silver-mostafa-dev/ml/models/fraud_scorecard/latest/model_scorecard.json - `
    --region us-east-1 |
    Select-String -Pattern "model_version|review_threshold|block_threshold"
