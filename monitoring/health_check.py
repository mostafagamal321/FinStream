import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.request import urlopen


@dataclass
class CheckResult:
    name: str
    status: str
    message: str


OK = "✅ OK"
WARN = "⚠️ WARN"
FAIL = "❌ FAIL"


def load_dotenv(path: str = ".env") -> Dict[str, str]:
    env = {}
    p = Path(path)

    if not p.exists():
        return env

    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()

        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip('"').strip("'")

    return env


ENV_FILE = load_dotenv()

BUCKET = (
    os.getenv("FINSTREAM_BRONZE_BUCKET")
    or os.getenv("S3_BUCKET")
    or os.getenv("BRONZE_BUCKET")
    or ENV_FILE.get("FINSTREAM_BRONZE_BUCKET")
    or ENV_FILE.get("S3_BUCKET")
    or ENV_FILE.get("BRONZE_BUCKET")
    or "finstream-bronze-mostafa-dev"
)

TODAY = os.getenv("FINSTREAM_HEALTH_DATE") or datetime.now(timezone.utc).strftime("%Y-%m-%d")

KAFKA_CONTAINER = os.getenv("KAFKA_CONTAINER", "finstream-kafka")
FLINK_URL = os.getenv("FLINK_URL", "http://localhost:8082")

MARKET_TOPIC = os.getenv("MARKET_TOPIC", "market_ticks_raw")
NEWS_TOPIC = os.getenv("NEWS_TOPIC", "news_raw")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "dead_letter_queue")

MARKET_CONSUMER_GROUP = os.getenv("MARKET_CONSUMER_GROUP", "flink-market-bronze-signals-job")
NEWS_CONSUMER_GROUP = os.getenv("NEWS_CONSUMER_GROUP", "flink-news-bronze-signals-job")

MARKET_S3_PREFIX = os.getenv(
    "MARKET_S3_PREFIX",
    f"bronze/market_ticks/dt={TODAY}/",
)

NEWS_S3_PREFIX = os.getenv(
    "NEWS_S3_PREFIX",
    f"bronze/market_news/dt={TODAY}/",
)

MARKET_MAX_AGE_SECONDS = int(os.getenv("MARKET_S3_MAX_AGE_SECONDS", "600"))

# News is not expected to land every few seconds like live market ticks.
# 21600 seconds = 6 hours.
NEWS_MAX_AGE_SECONDS = int(os.getenv("NEWS_S3_MAX_AGE_SECONDS", "21600"))

MAX_ALLOWED_LAG = int(os.getenv("MAX_ALLOWED_KAFKA_LAG", "1000"))

# DLQ check now uses growth, not total size.
# Existing old DLQ records will not make the pipeline unhealthy.
DLQ_GROWTH_WARN_THRESHOLD = int(os.getenv("DLQ_GROWTH_WARN_THRESHOLD", "100"))

STATE_FILE = Path(os.getenv("FINSTREAM_HEALTH_STATE_FILE", "monitoring/.health_state.json"))


def run_cmd(cmd: List[str], timeout: int = 30) -> Tuple[int, str, str]:
    try:
        completed = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            encoding="utf-8",
            errors="replace",
        )

        return completed.returncode, completed.stdout, completed.stderr

    except subprocess.TimeoutExpired as exc:
        return 124, exc.stdout or "", exc.stderr or f"Timeout after {timeout}s"

    except Exception as exc:
        return 1, "", str(exc)


def print_result(result: CheckResult) -> None:
    print(f"{result.status}  {result.name}")
    print(f"    {result.message}")


def load_state() -> Dict[str, object]:
    if not STATE_FILE.exists():
        return {}

    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_state(state: Dict[str, object]) -> None:
    try:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")
    except Exception:
        pass


def parse_compose_json_lines(output: str) -> List[Dict[str, object]]:
    output = output.strip()

    if not output:
        return []

    try:
        parsed = json.loads(output)

        if isinstance(parsed, list):
            return parsed

        if isinstance(parsed, dict):
            return [parsed]

    except Exception:
        pass

    items = []

    for line in output.splitlines():
        line = line.strip()

        if not line:
            continue

        try:
            item = json.loads(line)

            if isinstance(item, dict):
                items.append(item)

        except Exception:
            continue

    return items


def check_compose_service(service_name: str) -> CheckResult:
    code, out, err = run_cmd(
        ["docker", "compose", "ps", service_name, "--format", "json"],
        timeout=20,
    )

    if code != 0:
        return CheckResult(
            f"compose service:{service_name}",
            FAIL,
            err.strip() or out.strip(),
        )

    items = parse_compose_json_lines(out)

    if not items:
        return CheckResult(
            f"compose service:{service_name}",
            FAIL,
            "No container found for this compose service.",
        )

    healthy = []
    unhealthy = []

    for item in items:
        name = str(item.get("Name") or item.get("Service") or service_name)
        state = str(item.get("State") or "").lower()
        status = str(item.get("Status") or "")
        health = str(item.get("Health") or "").lower()

        is_running = state == "running"

        if is_running:
            if health and health not in {"healthy", "none", ""}:
                unhealthy.append(f"{name}: running but health={health}, status={status}")
            else:
                healthy.append(f"{name}: {status or 'running'}")
        else:
            unhealthy.append(f"{name}: state={state}, status={status}")

    if unhealthy:
        return CheckResult(
            f"compose service:{service_name}",
            FAIL,
            "; ".join(unhealthy),
        )

    return CheckResult(
        f"compose service:{service_name}",
        OK,
        "; ".join(healthy),
    )


def get_kafka_latest_offsets(topic: str) -> Tuple[Optional[int], str]:
    cmd = [
        "docker",
        "exec",
        "-i",
        KAFKA_CONTAINER,
        "kafka-run-class",
        "kafka.tools.GetOffsetShell",
        "--bootstrap-server",
        "kafka:9092",
        "--topic",
        topic,
        "--time",
        "-1",
    ]

    code, out, err = run_cmd(cmd, timeout=30)

    if code != 0:
        return None, err.strip() or out.strip()

    total = 0
    lines = [line.strip() for line in out.splitlines() if line.strip()]

    for line in lines:
        # Example:
        # market_ticks_raw:1:19318
        parts = line.split(":")

        if len(parts) >= 3:
            try:
                total += int(parts[-1])
            except ValueError:
                pass

    return total, out.strip()


def check_kafka_topic(topic: str) -> CheckResult:
    total, details = get_kafka_latest_offsets(topic)

    if total is None:
        return CheckResult(
            f"kafka topic:{topic}",
            FAIL,
            details,
        )

    if total == 0:
        return CheckResult(
            f"kafka topic:{topic}",
            WARN,
            "topic exists but latest total offset is 0",
        )

    return CheckResult(
        f"kafka topic:{topic}",
        OK,
        f"latest total offset = {total}",
    )


def parse_consumer_lag() -> Tuple[List[Dict[str, str]], str]:
    cmd = [
        "docker",
        "exec",
        "-i",
        KAFKA_CONTAINER,
        "kafka-consumer-groups",
        "--bootstrap-server",
        "kafka:9092",
        "--describe",
        "--all-groups",
    ]

    code, out, err = run_cmd(cmd, timeout=45)

    if code != 0:
        return [], err.strip() or out.strip()

    rows = []

    for line in out.splitlines():
        line = line.strip()

        if not line:
            continue

        if line.startswith("GROUP"):
            continue

        parts = re.split(r"\s+", line)

        if len(parts) < 6:
            continue

        group, topic, partition, current_offset, log_end_offset, lag = parts[:6]

        if topic in {MARKET_TOPIC, NEWS_TOPIC, DLQ_TOPIC}:
            rows.append(
                {
                    "group": group,
                    "topic": topic,
                    "partition": partition,
                    "current_offset": current_offset,
                    "log_end_offset": log_end_offset,
                    "lag": lag,
                }
            )

    return rows, out.strip()


def check_consumer_lag(topic: str, expected_group: str) -> CheckResult:
    rows, raw = parse_consumer_lag()

    if not rows and raw:
        return CheckResult(
            f"kafka consumer lag:{topic}",
            WARN,
            "No consumer group rows found. This may be normal if no job is consuming yet.",
        )

    topic_rows = [
        r
        for r in rows
        if r["topic"] == topic and r["group"] == expected_group
    ]

    if not topic_rows:
        return CheckResult(
            f"kafka consumer lag:{topic}",
            WARN,
            f"No consumer group rows found for expected group '{expected_group}'.",
        )

    max_lag = 0
    partitions = []

    for r in topic_rows:
        partitions.append(r["partition"])

        try:
            lag = int(r["lag"])
            max_lag = max(max_lag, lag)
        except ValueError:
            pass

    status = OK if max_lag <= MAX_ALLOWED_LAG else FAIL

    return CheckResult(
        f"kafka consumer lag:{topic}",
        status,
        (
            f"group={expected_group}, max_lag={max_lag}, "
            f"partitions={','.join(partitions)}"
        ),
    )


def parse_s3_ls(output: str) -> List[Dict[str, object]]:
    objects = []

    for line in output.splitlines():
        line = line.strip()

        if not line:
            continue

        # Example:
        # 2026-06-01 09:39:57       4039 bronze/market_ticks/dt=2026-06-01/part-...
        match = re.match(
            r"^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})\s+(\d+)\s+(.+)$",
            line,
        )

        if not match:
            continue

        date_str, time_str, size_str, key = match.groups()

        if key.endswith("_SUCCESS"):
            continue

        modified = datetime.strptime(
            f"{date_str} {time_str}",
            "%Y-%m-%d %H:%M:%S",
        ).replace(tzinfo=timezone.utc)

        objects.append(
            {
                "modified": modified,
                "size": int(size_str),
                "key": key,
            }
        )

    return objects


def check_s3_prefix(name: str, prefix: str, max_age_seconds: int) -> CheckResult:
    s3_uri = f"s3://{BUCKET}/{prefix}"

    cmd = [
        "docker",
        "run",
        "--rm",
        "--env-file",
        ".env",
        "amazon/aws-cli",
        "s3",
        "ls",
        s3_uri,
        "--recursive",
    ]

    code, out, err = run_cmd(cmd, timeout=60)

    if code != 0:
        return CheckResult(
            f"s3:{name}",
            FAIL,
            err.strip() or out.strip(),
        )

    objects = parse_s3_ls(out)

    if not objects:
        return CheckResult(
            f"s3:{name}",
            FAIL,
            f"No data objects found under {s3_uri}",
        )

    latest = max(objects, key=lambda x: x["modified"])
    total_size = sum(int(o["size"]) for o in objects)
    latest_age = int((datetime.now(timezone.utc) - latest["modified"]).total_seconds())

    status = OK if latest_age <= max_age_seconds else WARN

    return CheckResult(
        f"s3:{name}",
        status,
        (
            f"objects={len(objects)}, total_size={total_size} bytes, "
            f"latest_age={latest_age}s, max_allowed_age={max_age_seconds}s, "
            f"latest_key={latest['key']}"
        ),
    )


def check_dlq_growth(state: Dict[str, object]) -> CheckResult:
    total, details = get_kafka_latest_offsets(DLQ_TOPIC)

    if total is None:
        return CheckResult(
            f"kafka topic:{DLQ_TOPIC}",
            WARN,
            f"DLQ topic not found or not readable: {details}",
        )

    previous = state.get("dlq_total_offset")

    state["dlq_total_offset"] = total
    state["dlq_checked_at"] = datetime.now(timezone.utc).isoformat()

    if previous is None:
        return CheckResult(
            f"kafka topic:{DLQ_TOPIC}",
            OK,
            (
                f"DLQ total offset = {total}. Baseline saved. "
                "Future runs will warn only if DLQ grows unexpectedly."
            ),
        )

    try:
        previous_int = int(previous)
    except Exception:
        previous_int = total

    growth = total - previous_int

    if growth < 0:
        return CheckResult(
            f"kafka topic:{DLQ_TOPIC}",
            OK,
            f"DLQ total offset = {total}. Offset decreased/reset since previous baseline.",
        )

    if growth >= DLQ_GROWTH_WARN_THRESHOLD:
        return CheckResult(
            f"kafka topic:{DLQ_TOPIC}",
            WARN,
            (
                f"DLQ grew by {growth} records since last run. "
                f"current_total={total}, previous_total={previous_int}"
            ),
        )

    return CheckResult(
        f"kafka topic:{DLQ_TOPIC}",
        OK,
        (
            f"DLQ total offset = {total}, growth_since_last_run = {growth}. "
            "Existing old DLQ records are ignored."
        ),
    )


def check_flink_jobs() -> CheckResult:
    try:
        with urlopen(f"{FLINK_URL}/jobs/overview", timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))

    except Exception as exc:
        return CheckResult(
            "flink jobs",
            WARN,
            f"Cannot query Flink REST API at {FLINK_URL}: {exc}",
        )

    jobs = payload.get("jobs", [])

    if not jobs:
        return CheckResult(
            "flink jobs",
            FAIL,
            "No Flink jobs found.",
        )

    bad_jobs = []
    running_jobs = []

    for job in jobs:
        name = job.get("name", "unknown")
        state = job.get("state", "unknown")

        if state == "RUNNING":
            running_jobs.append(name)
        else:
            bad_jobs.append(f"{name}:{state}")

    if bad_jobs:
        return CheckResult(
            "flink jobs",
            FAIL,
            f"Non-running jobs: {', '.join(bad_jobs)}",
        )

    return CheckResult(
        "flink jobs",
        OK,
        f"running jobs: {', '.join(running_jobs)}",
    )


def main() -> int:
    state = load_state()

    print()
    print("FINSTREAM PIPELINE HEALTH CHECK")
    print("=" * 34)
    print(f"bucket: {BUCKET}")
    print(f"date:   {TODAY}")
    print()

    checks = [
        check_compose_service("kafka"),
        check_compose_service("flink-jobmanager"),
        check_compose_service("flink-taskmanager"),
        check_kafka_topic(MARKET_TOPIC),
        check_kafka_topic(NEWS_TOPIC),
        check_consumer_lag(MARKET_TOPIC, MARKET_CONSUMER_GROUP),
        check_consumer_lag(NEWS_TOPIC, NEWS_CONSUMER_GROUP),
        check_flink_jobs(),
        check_s3_prefix("market_ticks", MARKET_S3_PREFIX, MARKET_MAX_AGE_SECONDS),
        check_s3_prefix("market_news", NEWS_S3_PREFIX, NEWS_MAX_AGE_SECONDS),
        check_dlq_growth(state),
    ]

    save_state(state)

    print("Checks")
    print("-" * 34)

    for check in checks:
        print_result(check)

    print()
    print("Summary")
    print("-" * 34)

    has_fail = any(c.status == FAIL for c in checks)
    has_warn = any(c.status == WARN for c in checks)

    if has_fail:
        print("Overall: ❌ UNHEALTHY")
        return 2

    if has_warn:
        print("Overall: ⚠️ HEALTHY WITH WARNINGS")
        return 1

    print("Overall: ✅ HEALTHY")
    return 0


if __name__ == "__main__":
    sys.exit(main())