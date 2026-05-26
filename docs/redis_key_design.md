# Redis Key Design

Redis is the online feature store used by Flink during real-time scoring.

## Key Format

```text
<entity_type>:<entity_id>:<feature_group>