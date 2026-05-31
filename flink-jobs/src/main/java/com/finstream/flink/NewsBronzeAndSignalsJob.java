package com.finstream.flink;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * FinStream — NewsBronzeAndSignalsJob
 *
 * Current scope:
 *   news_raw Kafka topic
 *       │
 *       ├─► Validated + enriched records → bronze_news on S3
 *       │       - Rejects hard-invalid records before Bronze
 *       │       - Normalizes event_time to epoch milliseconds
 *       │       - Adds ingestion_time_ms
 *       │       - Adds source_lag_ms
 *       │       - Adds content_text
 *       │       - Adds sentiment_label
 *       │       - Adds risk_category
 *       │       - Adds severity
 *       │       - Adds negation_flag
 *       │       - Adds matched_keywords
 *       │       - Adds content_length
 *       │
 *       ├─► Bad records → news_dlq Kafka topic
 *       │       - Null/empty event_id
 *       │       - Null event_time
 *       │       - Null/empty title
 *       │       - Null/empty symbol_query
 *       │
 *       └─► Actionable signals → news_signals Kafka topic
 *               - CRITICAL/HIGH negative signals
 *               - MEDIUM positive signals
 *
 * Windowed aggregations are intentionally not included yet.
 */
public class NewsBronzeAndSignalsJob {

    public static void main(String[] args) throws Exception {

        String kafkaBootstrap = getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String schemaRegistryUrl = getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081");

        String inputTopic = getenv("TOPIC_NEWS_RAW", "news_raw");
        String signalsTopic = getenv("TOPIC_NEWS_SIGNALS", "news_signals");
        String dlqTopic = getenv("TOPIC_NEWS_DLQ", "dead_letter_queue");

        String s3Bucket = getenv(
                "FINSTREAM_BRONZE_BUCKET",
                getenv("S3_BUCKET", "finstream-bronze-mostafa-dev")
        );

        String s3NewsPath = "s3a://" + s3Bucket + "/bronze/market_news/";

        System.out.println("=== FinStream NewsBronzeAndSignalsJob ===");
        System.out.println("Kafka Bootstrap : " + kafkaBootstrap);
        System.out.println("Schema Registry : " + schemaRegistryUrl);
        System.out.println("Input Topic     : " + inputTopic);
        System.out.println("Signals Topic   : " + signalsTopic);
        System.out.println("DLQ Topic       : " + dlqTopic);
        System.out.println("S3 Bronze Path  : " + s3NewsPath);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(30_000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10_000);
        env.getCheckpointConfig().setCheckpointTimeout(60_000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        int parallelism = Integer.parseInt(getenv("FLINK_PARALLELISM", "4"));
        env.setParallelism(parallelism);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        tableEnv.getConfig()
                .getConfiguration()
                .setString("table.exec.state.ttl", "1 h");

        /*
         * Source table.
         *
         * event_time_ms normalizes event_time:
         *   - if event_time looks like epoch seconds, convert to milliseconds
         *   - otherwise keep it as epoch milliseconds
         */
        tableEnv.executeSql(
                "CREATE TABLE news_raw ("
                        + "event_id STRING,"
                        + "event_time BIGINT,"
                        + "published_at STRING,"
                        + "source_name STRING,"
                        + "author STRING,"
                        + "title STRING,"
                        + "description STRING,"
                        + "url STRING,"
                        + "symbol_query STRING,"
                        + "source STRING,"
                        + "raw_payload STRING,"

                        + "event_time_ms AS "
                        + "  CASE "
                        + "    WHEN event_time IS NULL THEN CAST(NULL AS BIGINT) "
                        + "    WHEN event_time < 10000000000 THEN event_time * 1000 "
                        + "    ELSE event_time "
                        + "  END,"

                        + "event_ts AS TO_TIMESTAMP_LTZ("
                        + "  CASE "
                        + "    WHEN event_time IS NULL THEN CAST(NULL AS BIGINT) "
                        + "    WHEN event_time < 10000000000 THEN event_time * 1000 "
                        + "    ELSE event_time "
                        + "  END, 3),"

                        + "WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND"
                        + ") WITH ("
                        + "'connector' = 'kafka',"
                        + "'topic' = '" + inputTopic + "',"
                        + "'properties.bootstrap.servers' = '" + kafkaBootstrap + "',"
                        + "'properties.group.id' = 'flink-news-bronze-signals-job',"
                        + "'properties.auto.offset.reset' = 'latest',"
                        + "'scan.startup.mode' = 'latest-offset',"
                        + "'format' = 'avro-confluent',"
                        + "'avro-confluent.schema-registry.url' = '" + schemaRegistryUrl + "'"
                        + ")"
        );

        /*
         * Bronze sink.
         *
         * Only valid enriched records land here.
         * This is the S3 source your Spark team should consume.
         */
        tableEnv.executeSql(
                "CREATE TABLE bronze_news ("
                        + "event_id STRING,"
                        + "event_time BIGINT,"
                        + "event_time_ms BIGINT,"
                        + "published_at STRING,"
                        + "source_name STRING,"
                        + "author STRING,"
                        + "title STRING,"
                        + "description STRING,"
                        + "url STRING,"
                        + "symbol_query STRING,"
                        + "source STRING,"
                        + "raw_payload STRING,"

                        + "content_text STRING,"
                        + "sentiment_label STRING,"
                        + "risk_category STRING,"
                        + "severity STRING,"
                        + "negation_flag BOOLEAN,"
                        + "matched_keywords STRING,"
                        + "content_length INT,"
                        + "ingestion_time_ms BIGINT,"
                        + "source_lag_ms BIGINT,"

                        + "dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "'connector' = 'filesystem',"
                        + "'path' = '" + s3NewsPath + "',"
                        + "'format' = 'parquet',"
                        + "'sink.partition-commit.policy.kind' = 'success-file',"
                        + "'sink.partition-commit.delay' = '1 min'"
                        + ")"
        );

        /*
         * Actionable signals sink.
         *
         * This should not receive every article.
         * Bronze keeps all valid articles; signals only receive useful alerts.
         */
        tableEnv.executeSql(
                "CREATE TABLE news_signals ("
                        + "signal_id STRING,"
                        + "signal_time BIGINT,"
                        + "title STRING,"
                        + "source_name STRING,"
                        + "published_at STRING,"
                        + "symbol_query STRING,"
                        + "sentiment_label STRING,"
                        + "risk_category STRING,"
                        + "signal_type STRING,"
                        + "severity STRING,"
                        + "matched_keywords STRING,"
                        + "negation_flag BOOLEAN,"
                        + "source_lag_ms BIGINT,"
                        + "reason STRING,"
                        + "source_event_id STRING,"
                        + "raw_event_source STRING"
                        + ") WITH ("
                        + "'connector' = 'kafka',"
                        + "'topic' = '" + signalsTopic + "',"
                        + "'properties.bootstrap.servers' = '" + kafkaBootstrap + "',"
                        + "'format' = 'json'"
                        + ")"
        );

        /*
         * DLQ sink.
         *
         * Invalid records are routed here instead of Bronze.
         */
        tableEnv.executeSql(
                "CREATE TABLE news_dlq ("
                        + "event_id STRING,"
                        + "original_event_time BIGINT,"
                        + "normalized_event_time_ms BIGINT,"
                        + "title STRING,"
                        + "symbol_query STRING,"
                        + "source_name STRING,"
                        + "source STRING,"
                        + "rejection_reason STRING,"
                        + "rejected_at_ms BIGINT,"
                        + "raw_payload STRING"
                        + ") WITH ("
                        + "'connector' = 'kafka',"
                        + "'topic' = '" + dlqTopic + "',"
                        + "'properties.bootstrap.servers' = '" + kafkaBootstrap + "',"
                        + "'format' = 'json'"
                        + ")"
        );

        /*
         * Valid records.
         */
        tableEnv.executeSql(
                "CREATE TEMPORARY VIEW valid_raw AS "
                        + "SELECT * "
                        + "FROM news_raw "
                        + "WHERE event_id IS NOT NULL "
                        + "  AND TRIM(event_id) <> '' "
                        + "  AND event_time_ms IS NOT NULL "
                        + "  AND title IS NOT NULL "
                        + "  AND TRIM(title) <> '' "
                        + "  AND symbol_query IS NOT NULL "
                        + "  AND TRIM(symbol_query) <> ''"
        );

        /*
         * Invalid records for DLQ.
         */
        tableEnv.executeSql(
                "CREATE TEMPORARY VIEW invalid_raw AS "
                        + "SELECT "
                        + "event_id,"
                        + "event_time AS original_event_time,"
                        + "event_time_ms AS normalized_event_time_ms,"
                        + "title,"
                        + "symbol_query,"
                        + "source_name,"
                        + "source,"
                        + "CASE "
                        + "  WHEN event_id IS NULL THEN 'NULL_EVENT_ID' "
                        + "  WHEN TRIM(event_id) = '' THEN 'EMPTY_EVENT_ID' "
                        + "  WHEN event_time_ms IS NULL THEN 'NULL_EVENT_TIME' "
                        + "  WHEN title IS NULL THEN 'NULL_TITLE' "
                        + "  WHEN TRIM(title) = '' THEN 'EMPTY_TITLE' "
                        + "  WHEN symbol_query IS NULL THEN 'NULL_SYMBOL_QUERY' "
                        + "  WHEN TRIM(symbol_query) = '' THEN 'EMPTY_SYMBOL_QUERY' "
                        + "  ELSE 'UNKNOWN_VALIDATION_ERROR' "
                        + "END AS rejection_reason,"
                        + "UNIX_TIMESTAMP() * 1000 AS rejected_at_ms,"
                        + "raw_payload "
                        + "FROM news_raw "
                        + "WHERE NOT ("
                        + "  event_id IS NOT NULL "
                        + "  AND TRIM(event_id) <> '' "
                        + "  AND event_time_ms IS NOT NULL "
                        + "  AND title IS NOT NULL "
                        + "  AND TRIM(title) <> '' "
                        + "  AND symbol_query IS NOT NULL "
                        + "  AND TRIM(symbol_query) <> ''"
                        + ")"
        );

        /*
         * Compute content_text once.
         */
        tableEnv.executeSql(
                "CREATE TEMPORARY VIEW news_with_content AS "
                        + "SELECT "
                        + "event_id,"
                        + "event_time,"
                        + "event_time_ms,"
                        + "published_at,"
                        + "source_name,"
                        + "author,"
                        + "title,"
                        + "description,"
                        + "url,"
                        + "symbol_query,"
                        + "source,"
                        + "raw_payload,"
                        + "event_ts,"
                        + "LOWER(CONCAT(title, ' ', COALESCE(description, ''))) AS content_text,"
                        + "CHAR_LENGTH(CONCAT(title, ' ', COALESCE(description, ''))) AS content_length,"
                        + "UNIX_TIMESTAMP() * 1000 AS ingestion_time_ms,"
                        + "UNIX_TIMESTAMP() * 1000 - event_time_ms AS source_lag_ms "
                        + "FROM valid_raw"
        );

        /*
         * Enrichment view.
         *
         * Negation detection is intentionally phrase-based.
         * Avoid broad patterns like '%no %' because they create false neutral results.
         */
        tableEnv.executeSql(
                "CREATE TEMPORARY VIEW enriched_news AS "
                        + "SELECT "
                        + "n.*,"

                        + "CASE "
                        + "  WHEN content_text LIKE '%no fraud%' "
                        + "    OR content_text LIKE '%not fraud%' "
                        + "    OR content_text LIKE '%not a fraud%' "
                        + "    OR content_text LIKE '%no evidence of fraud%' "
                        + "    OR content_text LIKE '%without evidence of fraud%' "
                        + "    OR content_text LIKE '%denies fraud%' "
                        + "    OR content_text LIKE '%denied fraud%' "
                        + "    OR content_text LIKE '%refutes fraud%' "
                        + "    OR content_text LIKE '%dismisses fraud%' "
                        + "    OR content_text LIKE '%no lawsuit%' "
                        + "    OR content_text LIKE '%not sued%' "
                        + "    OR content_text LIKE '%not under investigation%' "
                        + "    OR content_text LIKE '%no investigation%' "
                        + "    OR content_text LIKE '%not a crash%' "
                        + "    OR content_text LIKE '%no crash%' "
                        + "  THEN TRUE "
                        + "  ELSE FALSE "
                        + "END AS negation_flag,"

                        + "CASE "
                        + "  WHEN content_text LIKE '%fraud%' "
                        + "    OR content_text LIKE '%ponzi%' "
                        + "    OR content_text LIKE '%embezzlement%' "
                        + "    OR content_text LIKE '%money laundering%' "
                        + "  THEN 'FRAUD' "

                        + "  WHEN content_text LIKE '%lawsuit%' "
                        + "    OR content_text LIKE '%investigation%' "
                        + "    OR content_text LIKE '%sec charges%' "
                        + "    OR content_text LIKE '%indicted%' "
                        + "    OR content_text LIKE '%settlement%' "
                        + "  THEN 'LEGAL' "

                        + "  WHEN content_text LIKE '%crash%' "
                        + "    OR content_text LIKE '%recession%' "
                        + "    OR content_text LIKE '%market collapse%' "
                        + "    OR content_text LIKE '%bank run%' "
                        + "    OR content_text LIKE '%default%' "
                        + "  THEN 'MARKET_CRASH' "

                        + "  WHEN content_text LIKE '%rate hike%' "
                        + "    OR content_text LIKE '%interest rate%' "
                        + "    OR content_text LIKE '%federal reserve%' "
                        + "    OR content_text LIKE '%regulation%' "
                        + "    OR content_text LIKE '%central bank%' "
                        + "  THEN 'REGULATORY' "

                        + "  WHEN content_text LIKE '%growth%' "
                        + "    OR content_text LIKE '%profit%' "
                        + "    OR content_text LIKE '%rally%' "
                        + "    OR content_text LIKE '%surge%' "
                        + "    OR content_text LIKE '%record high%' "
                        + "    OR content_text LIKE '%beat expectations%' "
                        + "  THEN 'POSITIVE' "

                        + "  ELSE 'GENERAL' "
                        + "END AS risk_category,"

                        + "TRIM(BOTH ',' FROM CONCAT("
                        + "  CASE WHEN content_text LIKE '%fraud%' THEN 'fraud,' ELSE '' END,"
                        + "  CASE WHEN content_text LIKE '%ponzi%' THEN 'ponzi,' ELSE '' END,"
                        + "  CASE WHEN content_text LIKE '%embezzlement%' THEN 'embezzlement,' ELSE '' END,"
                        + "  CASE WHEN content_text LIKE '%money laundering%' THEN 'money_laundering,' ELSE '' END,"
                        + "  CASE WHEN content_text LIKE '%lawsuit%' THEN 'lawsuit,' ELSE '' END,"
                        + "  CASE WHEN content_text LIKE '%investigation%' THEN 'investigation,' ELSE '' END,"
                        + "  CASE WHEN content_text LIKE '%sec charges%' THEN 'sec_charges,' ELSE '' END,"
                        + "  CASE WHEN content_text LIKE '%indicted%' THEN 'indicted,' ELSE '' END,"
                        + "  CASE WHEN content_text LIKE '%settlement%' THEN 'settlement,' ELSE '' END,"
                        + "  CASE WHEN content_text LIKE '%crash%' THEN 'crash,' ELSE '' END,"
                        + "  CASE WHEN content_text LIKE '%recession%' THEN 'recession,' ELSE '' END,"
                        + "  CASE WHEN content_text LIKE '%market collapse%' THEN 'market_collapse,' ELSE '' END,"
                        + "  CASE WHEN content_text LIKE '%bank run%' THEN 'bank_run,' ELSE '' END,"
                        + "  CASE WHEN content_text LIKE '%default%' THEN 'default,' ELSE '' END,"
                        + "  CASE WHEN content_text LIKE '%rate hike%' THEN 'rate_hike,' ELSE '' END,"
                        + "  CASE WHEN content_text LIKE '%interest rate%' THEN 'interest_rate,' ELSE '' END,"
                        + "  CASE WHEN content_text LIKE '%federal reserve%' THEN 'federal_reserve,' ELSE '' END,"
                        + "  CASE WHEN content_text LIKE '%regulation%' THEN 'regulation,' ELSE '' END,"
                        + "  CASE WHEN content_text LIKE '%central bank%' THEN 'central_bank,' ELSE '' END,"
                        + "  CASE WHEN content_text LIKE '%growth%' THEN 'growth,' ELSE '' END,"
                        + "  CASE WHEN content_text LIKE '%profit%' THEN 'profit,' ELSE '' END,"
                        + "  CASE WHEN content_text LIKE '%rally%' THEN 'rally,' ELSE '' END,"
                        + "  CASE WHEN content_text LIKE '%surge%' THEN 'surge,' ELSE '' END,"
                        + "  CASE WHEN content_text LIKE '%record high%' THEN 'record_high,' ELSE '' END,"
                        + "  CASE WHEN content_text LIKE '%beat expectations%' THEN 'beat_expectations,' ELSE '' END"
                        + ")) AS matched_keywords "

                        + "FROM news_with_content n"
        );

        /*
         * Final classification.
         */
        tableEnv.executeSql(
                "CREATE TEMPORARY VIEW final_classified AS "
                        + "SELECT "
                        + "e.*,"

                        + "CASE "
                        + "  WHEN negation_flag = TRUE THEN 'NEUTRAL' "
                        + "  WHEN risk_category IN ('FRAUD', 'LEGAL', 'MARKET_CRASH') THEN 'NEGATIVE' "
                        + "  WHEN risk_category = 'REGULATORY' THEN 'NEUTRAL' "
                        + "  WHEN risk_category = 'POSITIVE' THEN 'POSITIVE' "
                        + "  ELSE 'NEUTRAL' "
                        + "END AS sentiment_label,"

                        + "CASE "
                        + "  WHEN negation_flag = FALSE AND risk_category = 'FRAUD' THEN 'CRITICAL' "
                        + "  WHEN negation_flag = FALSE AND risk_category IN ('LEGAL', 'MARKET_CRASH') THEN 'HIGH' "
                        + "  WHEN negation_flag = FALSE AND risk_category = 'REGULATORY' THEN 'MEDIUM' "
                        + "  WHEN risk_category = 'POSITIVE' THEN 'MEDIUM' "
                        + "  ELSE 'LOW' "
                        + "END AS severity "

                        + "FROM enriched_news e"
        );

        StatementSet stmts = tableEnv.createStatementSet();

        /*
         * Valid enriched news → S3 Bronze.
         */
        stmts.addInsertSql(
                "INSERT INTO bronze_news "
                        + "SELECT "
                        + "event_id,"
                        + "event_time,"
                        + "event_time_ms,"
                        + "published_at,"
                        + "source_name,"
                        + "author,"
                        + "title,"
                        + "description,"
                        + "url,"
                        + "symbol_query,"
                        + "source,"
                        + "raw_payload,"
                        + "content_text,"
                        + "sentiment_label,"
                        + "risk_category,"
                        + "severity,"
                        + "negation_flag,"
                        + "matched_keywords,"
                        + "content_length,"
                        + "ingestion_time_ms,"
                        + "source_lag_ms,"
                        + "DATE_FORMAT(event_ts, 'yyyy-MM-dd') AS dt "
                        + "FROM final_classified"
        );

        /*
         * Actionable news signals → Kafka.
         *
         * Bronze keeps all valid records.
         * Signals keeps only records useful for alerting/market intelligence.
         */
        stmts.addInsertSql(
                "INSERT INTO news_signals "
                        + "SELECT "
                        + "CONCAT('news-', symbol_query, '-', CAST(event_time_ms AS STRING), '-', CAST(ABS(HASH_CODE(event_id)) AS STRING)) AS signal_id,"
                        + "event_time_ms AS signal_time,"
                        + "title,"
                        + "source_name,"
                        + "published_at,"
                        + "symbol_query,"
                        + "sentiment_label,"
                        + "risk_category,"
                        + "CASE "
                        + "  WHEN risk_category = 'FRAUD' THEN 'FRAUD_SIGNAL' "
                        + "  WHEN risk_category = 'LEGAL' THEN 'LEGAL_SIGNAL' "
                        + "  WHEN risk_category = 'MARKET_CRASH' THEN 'CRASH_SIGNAL' "
                        + "  WHEN risk_category = 'REGULATORY' THEN 'REGULATORY_SIGNAL' "
                        + "  WHEN risk_category = 'POSITIVE' THEN 'POSITIVE_SIGNAL' "
                        + "  ELSE 'NEWS_MENTION' "
                        + "END AS signal_type,"
                        + "severity,"
                        + "matched_keywords,"
                        + "negation_flag,"
                        + "source_lag_ms,"
                        + "CONCAT("
                        + "  risk_category, ' detected for ', symbol_query, "
                        + "  ' | keywords: ', "
                        + "  CASE WHEN matched_keywords IS NULL OR matched_keywords = '' THEN 'none' ELSE matched_keywords END, "
                        + "  CASE WHEN negation_flag THEN ' | negation detected, downgraded' ELSE '' END"
                        + ") AS reason,"
                        + "event_id AS source_event_id,"
                        + "source AS raw_event_source "
                        + "FROM final_classified "
                        + "WHERE severity IN ('CRITICAL', 'HIGH', 'MEDIUM') "
                        + "  AND sentiment_label IN ('NEGATIVE', 'POSITIVE')"
        );

        /*
         * Invalid news → DLQ.
         */
        stmts.addInsertSql(
                "INSERT INTO news_dlq "
                        + "SELECT "
                        + "event_id,"
                        + "original_event_time,"
                        + "normalized_event_time_ms,"
                        + "title,"
                        + "symbol_query,"
                        + "source_name,"
                        + "source,"
                        + "rejection_reason,"
                        + "rejected_at_ms,"
                        + "raw_payload "
                        + "FROM invalid_raw"
        );

        stmts.execute();
    }

    private static String getenv(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}