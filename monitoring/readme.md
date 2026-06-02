# FinStream Health Check

This folder contains simple monitoring scripts for the FinStream pipeline.

The main script is:

```text
monitoring/health_check.py
```

### Why we use it

Before this script, we had to manually run many commands:

```text
docker compose logs
kafka-console-consumer
kafka-consumer-groups
aws s3 ls
python read_parquet checks
```

This script combines the important checks into one command.
