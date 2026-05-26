# Phase 1 — Architecture, Contracts, and Naming Standards

## Goal

Phase 1 defines the shared contracts used by all FinStream components before implementation starts.

These contracts include:

- Kafka topic names
- Avro schema names
- Schema Registry subject names
- S3 lakehouse layout
- Redis online feature keys
- ClickHouse serving tables
- Service ports
- Naming conventions

## Rule

No producer, Flink job, Spark job, dbt model, dashboard, or alert consumer should invent its own topic, table, bucket, or key name.

All implementation must follow the contracts in this folder.

## Phase 1 Output Files

| File | Purpose |
|---|---|
| `kafka_topics.md` | Kafka topic contracts |
| `schema_registry.md` | Avro and Schema Registry rules |
| `s3_lakehouse_layout.md` | Bronze, Silver, Gold, checkpoints, and ML artifacts |
| `redis_key_design.md` | Online feature store key design |
| `clickhouse_tables.md` | Realtime serving table definitions |
| `service_ports.md` | Local service URLs |