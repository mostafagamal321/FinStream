
-- ============================================================
-- FinStream Live Fraud Analytics Materialized Views
-- Source table:
--   finstream.fraud_scores_rt_enriched
--
-- Modeling:
--   - AggregatingMergeTree for metrics that need avg/max
--   - SummingMergeTree only for pure counts
-- ============================================================


-- ============================================================
-- 1) Decision + Risk aggregates per minute
-- Answers:
--   - How many APPROVE / REVIEW / BLOCK per minute?
--   - Average fraud score per decision/risk level?
--   - Maximum fraud score seen per minute?
-- ============================================================

DROP VIEW IF EXISTS finstream.mv_fraud_decisions_1m;
DROP TABLE IF EXISTS finstream.fraud_decisions_1m;

CREATE TABLE finstream.fraud_decisions_1m
(
    minute DateTime('UTC'),
    decision LowCardinality(String),
    risk_level LowCardinality(String),

    total_state AggregateFunction(count),
    sum_fraud_score_state AggregateFunction(sum, Float64),
    max_fraud_score_state AggregateFunction(max, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(minute)
ORDER BY (minute, decision, risk_level)
TTL minute + INTERVAL 14 DAY;

CREATE MATERIALIZED VIEW finstream.mv_fraud_decisions_1m
TO finstream.fraud_decisions_1m
AS
SELECT
    toStartOfMinute(inserted_at) AS minute,
    decision,
    risk_level,
    countState() AS total_state,
    sumState(toFloat64(fraud_score)) AS sum_fraud_score_state,
    maxState(toFloat64(fraud_score)) AS max_fraud_score_state
FROM finstream.fraud_scores_rt_enriched
GROUP BY minute, decision, risk_level;


-- ============================================================
-- 2) Top block reasons per minute
-- Answers:
--   - What is the most common reason causing BLOCK?
-- ============================================================

DROP VIEW IF EXISTS finstream.mv_block_reasons_1m;
DROP TABLE IF EXISTS finstream.block_reasons_1m;

CREATE TABLE finstream.block_reasons_1m
(
    minute DateTime('UTC'),
    reason LowCardinality(String),
    total UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toDate(minute)
ORDER BY (minute, reason)
TTL minute + INTERVAL 14 DAY;

CREATE MATERIALIZED VIEW finstream.mv_block_reasons_1m
TO finstream.block_reasons_1m
AS
SELECT
    toStartOfMinute(inserted_at) AS minute,
    arrayJoin(arrayFilter(x -> x != '', splitByChar(',', reason_codes))) AS reason,
    count() AS total
FROM finstream.fraud_scores_rt_enriched
WHERE decision = 'BLOCK'
GROUP BY minute, reason;


-- ============================================================
-- 3) Device risk per minute
-- Answers:
--   - Which devices generate the most transactions?
--   - Which devices are blocked/reviewed the most?
--   - What is the average fraud score by device?
-- ============================================================

DROP VIEW IF EXISTS finstream.mv_device_risk_1m;
DROP TABLE IF EXISTS finstream.device_risk_1m;

CREATE TABLE finstream.device_risk_1m
(
    minute DateTime('UTC'),
    device_type LowCardinality(String),
    decision LowCardinality(String),
    risk_level LowCardinality(String),

    total_state AggregateFunction(count),
    sum_fraud_score_state AggregateFunction(sum, Float64),
    max_fraud_score_state AggregateFunction(max, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(minute)
ORDER BY (minute, device_type, decision, risk_level)
TTL minute + INTERVAL 14 DAY;

CREATE MATERIALIZED VIEW finstream.mv_device_risk_1m
TO finstream.device_risk_1m
AS
SELECT
    toStartOfMinute(inserted_at) AS minute,
    ifNull(device_type, 'UNKNOWN') AS device_type,
    decision,
    risk_level,
    countState() AS total_state,
    sumState(toFloat64(fraud_score)) AS sum_fraud_score_state,
    maxState(toFloat64(fraud_score)) AS max_fraud_score_state
FROM finstream.fraud_scores_rt_enriched
GROUP BY minute, device_type, decision, risk_level;


-- ============================================================
-- 4) Amount bucket risk per minute
-- Answers:
--   - Are high-value transactions more likely to be REVIEW/BLOCK?
--   - Which amount ranges are riskier in the live stream?
-- ============================================================

DROP VIEW IF EXISTS finstream.mv_amount_risk_1m;
DROP TABLE IF EXISTS finstream.amount_risk_1m;

CREATE TABLE finstream.amount_risk_1m
(
    minute DateTime('UTC'),
    amount_bucket LowCardinality(String),
    decision LowCardinality(String),
    risk_level LowCardinality(String),

    total_state AggregateFunction(count),
    sum_fraud_score_state AggregateFunction(sum, Float64),
    max_fraud_score_state AggregateFunction(max, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(minute)
ORDER BY (minute, amount_bucket, decision, risk_level)
TTL minute + INTERVAL 14 DAY;

CREATE MATERIALIZED VIEW finstream.mv_amount_risk_1m
TO finstream.amount_risk_1m
AS
SELECT
    toStartOfMinute(inserted_at) AS minute,
    multiIf(
        amount < 50, '0-50',
        amount < 100, '50-100',
        amount < 500, '100-500',
        amount < 1000, '500-1000',
        '1000+'
    ) AS amount_bucket,
    decision,
    risk_level,
    countState() AS total_state,
    sumState(toFloat64(fraud_score)) AS sum_fraud_score_state,
    maxState(toFloat64(fraud_score)) AS max_fraud_score_state
FROM finstream.fraud_scores_rt_enriched
GROUP BY minute, amount_bucket, decision, risk_level;


-- ============================================================
-- 5) Address status risk per minute
-- Answers:
--   - Does missing address data correlate with REVIEW/BLOCK?
--   - Are transactions with missing addr1/addr2 riskier?
-- ============================================================

DROP VIEW IF EXISTS finstream.mv_address_risk_1m;
DROP TABLE IF EXISTS finstream.address_risk_1m;

CREATE TABLE finstream.address_risk_1m
(
    minute DateTime('UTC'),
    addr1_status LowCardinality(String),
    addr2_status LowCardinality(String),
    decision LowCardinality(String),
    risk_level LowCardinality(String),

    total_state AggregateFunction(count),
    sum_fraud_score_state AggregateFunction(sum, Float64),
    max_fraud_score_state AggregateFunction(max, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(minute)
ORDER BY (minute, addr1_status, addr2_status, decision, risk_level)
TTL minute + INTERVAL 14 DAY;

CREATE MATERIALIZED VIEW finstream.mv_address_risk_1m
TO finstream.address_risk_1m
AS
SELECT
    toStartOfMinute(inserted_at) AS minute,
    if(isNull(addr1), 'MISSING_ADDR1', 'HAS_ADDR1') AS addr1_status,
    if(isNull(addr2), 'MISSING_ADDR2', 'HAS_ADDR2') AS addr2_status,
    decision,
    risk_level,
    countState() AS total_state,
    sumState(toFloat64(fraud_score)) AS sum_fraud_score_state,
    maxState(toFloat64(fraud_score)) AS max_fraud_score_state
FROM finstream.fraud_scores_rt_enriched
GROUP BY minute, addr1_status, addr2_status, decision, risk_level;
