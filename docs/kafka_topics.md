# Kafka Topic Contracts

## Final Topic List

| Topic | Key | Value Format | Producer | Consumer |
|---|---|---|---|---|
| `transactions_raw` | `transaction_id` | Avro | Transaction producer | Flink transaction scoring job |
| `market_ticks_raw` | `symbol` | Avro | Market producer | Flink market signal job |
| `news_raw` | `article_id` or `symbol` | Avro | News producer | Flink market signal job |
| `fx_rates_raw` | `currency_pair` | Avro | FX producer | Flink market signal job |
| `fraud_alerts` | `transaction_id` | Avro | Flink transaction scoring job | Alert consumer |
| `dead_letter_queue` | `event_id` | Avro or JSON | Producers/Flink | Debugging and monitoring |
| `raw_audit_events` | `event_id` | Avro | Flink | Lakehouse/audit path |

## Naming Rule

Use:

```text
<entity>_<stage>