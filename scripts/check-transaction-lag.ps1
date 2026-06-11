$ErrorActionPreference = "Stop"

docker exec -i finstream-kafka kafka-consumer-groups `
    --bootstrap-server kafka:9092 `
    --describe `
    --group finstream-transactions-stream-v1 `
    --timeout 30000
