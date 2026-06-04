CREATE OR REPLACE VIEW finstream.v_rule_performance_by_label AS
SELECT
    risk_level,
    decision,
    actual_is_fraud,
    count() AS cnt,
    round(cnt / (SELECT count() FROM finstream.fraud_scores_rt_latest), 4) AS pct_of_total
FROM finstream.fraud_scores_rt_latest
GROUP BY
    risk_level,
    decision,
    actual_is_fraud
ORDER BY
    risk_level,
    decision,
    actual_is_fraud;

CREATE OR REPLACE VIEW finstream.v_rule_confusion_summary AS
SELECT
    countIf(actual_is_fraud = 1 AND decision = 'BLOCK') AS true_positives,
    countIf(actual_is_fraud = 0 AND decision = 'BLOCK') AS false_positives,
    countIf(actual_is_fraud = 1 AND decision != 'BLOCK') AS false_negatives,
    countIf(actual_is_fraud = 0 AND decision != 'BLOCK') AS true_negatives,
    round(true_positives / nullIf(true_positives + false_negatives, 0), 4) AS recall,
    round(true_positives / nullIf(true_positives + false_positives, 0), 4) AS precision,
    round(false_positives / nullIf(false_positives + true_negatives, 0), 4) AS false_positive_rate
FROM finstream.fraud_scores_rt_latest;

CREATE OR REPLACE VIEW finstream.v_fraud_label_distribution AS
SELECT
    actual_is_fraud,
    count() AS cnt,
    round(cnt / (SELECT count() FROM finstream.fraud_scores_rt_latest), 4) AS pct
FROM finstream.fraud_scores_rt_latest
GROUP BY actual_is_fraud
ORDER BY actual_is_fraud;