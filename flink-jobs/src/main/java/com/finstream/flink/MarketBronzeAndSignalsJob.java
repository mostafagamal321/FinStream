package com.finstream.flink;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * FinStream — MarketBronzeAndSignalsJob
 *
 * Current scope:
 *   market_ticks_raw Kafka topic
 *       │
 *       ├─► Validated + enriched records → bronze_market_ticks on S3
 *       │       - Rejects hard-invalid records before Bronze
 *       │       - Adds normalized event time
 *       │       - Adds ingestion_time_ms
 *       │       - Adds source_lag_ms
 *       │       - Adds anomaly_flag
 *       │
 *       ├─► Bad records → market_ticks_dlq Kafka topic
 *       │       - Null/empty symbol
 *       │       - Missing or invalid event_time
 *       │       - Null/non-positive price
 *       │       - Null/negative volume
 *       │       - Invalid OHLC fields
 *       │
 *       └─► Signals → market_signals Kafka topic
 *               - PRICE_ABOVE_HIGH
 *               - PRICE_BELOW_LOW
 *               - HIGH_VOLUME
 *               - LATE_SOURCE_EVENT
 *               - MARKET_TICK
 *
 * Windowed aggregations are intentionally not included yet.
 */
public class MarketBronzeAndSignalsJob {

    private static final long HIGH_VOLUME_THRESHOLD = 1_000_000L;
    private static final long LATE_SOURCE_EVENT_THRESHOLD_MS = 300_000L; // 5 minutes

    public static void main(String[] args) {
        String kafkaBootstrap = getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String schemaRegistryUrl = getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081");

        String inputTopic = getenv("TOPIC_MARKET_TICKS_RAW", "market_ticks_raw");
        String signalsTopic = getenv("TOPIC_MARKET_SIGNALS", "market_signals");
        String dlqTopic = getenv("TOPIC_MARKET_TICKS_DLQ", getenv("TOPIC_MARKET_DLQ", "dead_letter_queue"));

        String s3Bucket = getenv(
                "FINSTREAM_BRONZE_BUCKET",
                getenv("S3_BUCKET", "finstream-bronze-mostafa-dev")
        );

        String s3MarketPath = "s3a://" + s3Bucket + "/bronze/market_ticks/";

        System.out.println("=== FinStream MarketBronzeAndSignalsJob ===");
        System.out.println("Kafka Bootstrap    : " + kafkaBootstrap);
        System.out.println("Schema Registry    : " + schemaRegistryUrl);
        System.out.println("Input Topic        : " + inputTopic);
        System.out.println("Signals Topic      : " + signalsTopic);
        System.out.println("DLQ Topic          : " + dlqTopic);
        System.out.println("S3 Bronze Path     : " + s3MarketPath);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(30_000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10_000);
        env.getCheckpointConfig().setCheckpointTimeout(60_000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        int parallelism = Integer.parseInt(getenv("FLINK_PARALLELISM", "4"));
        env.setParallelism(parallelism);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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
                "CREATE TABLE market_ticks_raw ("
                        + "event_id STRING,"
                        + "event_time BIGINT,"
                        + "symbol STRING,"
                        + "price DOUBLE,"
                        + "open_price DOUBLE,"
                        + "high_price DOUBLE,"
                        + "low_price DOUBLE,"
                        + "close_price DOUBLE,"
                        + "volume DOUBLE,"
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

                        + "proc_time AS PROCTIME(),"
                        + "WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND"
                        + ") WITH ("
                        + "'connector' = 'kafka',"
                        + "'topic' = '" + inputTopic + "',"
                        + "'properties.bootstrap.servers' = '" + kafkaBootstrap + "',"
                        + "'properties.group.id' = 'flink-market-bronze-signals-job',"
                        + "'properties.auto.offset.reset' = 'latest',"
                        + "'scan.startup.mode' = 'latest-offset',"
                        + "'format' = 'avro-confluent',"
                        + "'avro-confluent.schema-registry.url' = '" + schemaRegistryUrl + "'"
                        + ")"
        );

        /*
         * Bronze sink.
         *
         * Only valid records land here.
         * Bronze still keeps raw_payload for replay/audit.
         */
        tableEnv.executeSql(
                "CREATE TABLE bronze_market_ticks ("
                        + "event_id STRING,"
                        + "event_time BIGINT,"
                        + "event_time_ms BIGINT,"
                        + "ingestion_time_ms BIGINT,"
                        + "source_lag_ms BIGINT,"
                        + "symbol STRING,"
                        + "price DOUBLE,"
                        + "open_price DOUBLE,"
                        + "high_price DOUBLE,"
                        + "low_price DOUBLE,"
                        + "close_price DOUBLE,"
                        + "volume DOUBLE,"
                        + "source STRING,"
                        + "anomaly_flag STRING,"
                        + "raw_payload STRING,"
                        + "dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "'connector' = 'filesystem',"
                        + "'path' = '" + s3MarketPath + "',"
                        + "'format' = 'parquet',"
                        + "'sink.partition-commit.policy.kind' = 'success-file',"
                        + "'sink.partition-commit.delay' = '1 min'"
                        + ")"
        );

        /*
         * Signals sink.
         */
        tableEnv.executeSql(
                "CREATE TABLE market_signals ("
                        + "signal_id STRING,"
                        + "signal_time BIGINT,"
                        + "symbol STRING,"
                        + "current_price DOUBLE,"
                        + "volume DOUBLE,"
                        + "anomaly_flag STRING,"
                        + "source_lag_ms BIGINT,"
                        + "signal_type STRING,"
                        + "severity STRING,"
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
         * Invalid records are sent here instead of being written to Bronze.
         */
        tableEnv.executeSql(
                "CREATE TABLE market_ticks_dlq ("
                        + "event_id STRING,"
                        + "original_event_time BIGINT,"
                        + "normalized_event_time_ms BIGINT,"
                        + "symbol STRING,"
                        + "price DOUBLE,"
                        + "open_price DOUBLE,"
                        + "high_price DOUBLE,"
                        + "low_price DOUBLE,"
                        + "close_price DOUBLE,"
                        + "volume DOUBLE,"
                        + "source STRING,"
                        + "validation_error STRING,"
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
         *
         * Hard-invalid records are excluded before Bronze.
         */
        tableEnv.executeSql(
                "CREATE TEMPORARY VIEW valid_market_ticks AS "
                        + "SELECT * "
                        + "FROM market_ticks_raw "
                        + "WHERE event_id IS NOT NULL "
                        + "  AND event_time_ms IS NOT NULL "
                        + "  AND symbol IS NOT NULL "
                        + "  AND TRIM(symbol) <> '' "
                        + "  AND price IS NOT NULL "
                        + "  AND price > 0 "
                        + "  AND volume IS NOT NULL "
                        + "  AND volume >= 0 "
                        + "  AND open_price IS NOT NULL "
                        + "  AND high_price IS NOT NULL "
                        + "  AND low_price IS NOT NULL "
                        + "  AND close_price IS NOT NULL "
                        + "  AND open_price > 0 "
                        + "  AND high_price > 0 "
                        + "  AND low_price > 0 "
                        + "  AND close_price > 0 "
                        + "  AND high_price >= low_price"
        );

        /*
         * Invalid records.
         *
         * These go to DLQ.
         */
        tableEnv.executeSql(
                "CREATE TEMPORARY VIEW invalid_market_ticks AS "
                        + "SELECT "
                        + "event_id,"
                        + "event_time AS original_event_time,"
                        + "event_time_ms AS normalized_event_time_ms,"
                        + "symbol,"
                        + "price,"
                        + "open_price,"
                        + "high_price,"
                        + "low_price,"
                        + "close_price,"
                        + "volume,"
                        + "source,"
                        + "CASE "
                        + "  WHEN event_id IS NULL THEN 'NULL_EVENT_ID' "
                        + "  WHEN event_time_ms IS NULL THEN 'NULL_EVENT_TIME' "
                        + "  WHEN symbol IS NULL OR TRIM(symbol) = '' THEN 'NULL_OR_EMPTY_SYMBOL' "
                        + "  WHEN price IS NULL THEN 'NULL_PRICE' "
                        + "  WHEN price <= 0 THEN 'NON_POSITIVE_PRICE' "
                        + "  WHEN volume IS NULL THEN 'NULL_VOLUME' "
                        + "  WHEN volume < 0 THEN 'NEGATIVE_VOLUME' "
                        + "  WHEN open_price IS NULL OR high_price IS NULL OR low_price IS NULL OR close_price IS NULL THEN 'NULL_OHLC_VALUE' "
                        + "  WHEN open_price <= 0 OR high_price <= 0 OR low_price <= 0 OR close_price <= 0 THEN 'NON_POSITIVE_OHLC_VALUE' "
                        + "  WHEN high_price < low_price THEN 'HIGH_LESS_THAN_LOW' "
                        + "  ELSE 'UNKNOWN_VALIDATION_ERROR' "
                        + "END AS validation_error,"
                        + "UNIX_TIMESTAMP() * 1000 AS rejected_at_ms,"
                        + "raw_payload "
                        + "FROM market_ticks_raw "
                        + "WHERE NOT ("
                        + "  event_id IS NOT NULL "
                        + "  AND event_time_ms IS NOT NULL "
                        + "  AND symbol IS NOT NULL "
                        + "  AND TRIM(symbol) <> '' "
                        + "  AND price IS NOT NULL "
                        + "  AND price > 0 "
                        + "  AND volume IS NOT NULL "
                        + "  AND volume >= 0 "
                        + "  AND open_price IS NOT NULL "
                        + "  AND high_price IS NOT NULL "
                        + "  AND low_price IS NOT NULL "
                        + "  AND close_price IS NOT NULL "
                        + "  AND open_price > 0 "
                        + "  AND high_price > 0 "
                        + "  AND low_price > 0 "
                        + "  AND close_price > 0 "
                        + "  AND high_price >= low_price"
                        + ")"
        );

        /*
         * Enriched valid records.
         *
         * This is the shared view used by both Bronze and signals.
         */
        tableEnv.executeSql(
                "CREATE TEMPORARY VIEW enriched_market_ticks AS "
                        + "SELECT "
                        + "event_id,"
                        + "event_time,"
                        + "event_time_ms,"
                        + "UNIX_TIMESTAMP() * 1000 AS ingestion_time_ms,"
                        + "UNIX_TIMESTAMP() * 1000 - event_time_ms AS source_lag_ms,"
                        + "symbol,"
                        + "price,"
                        + "open_price,"
                        + "high_price,"
                        + "low_price,"
                        + "close_price,"
                        + "volume,"
                        + "source,"
                        + "raw_payload,"
                        + "event_ts,"
                        + "CASE "
                        + "  WHEN price > high_price THEN 'PRICE_ABOVE_HIGH' "
                        + "  WHEN price < low_price THEN 'PRICE_BELOW_LOW' "
                        + "  WHEN UNIX_TIMESTAMP() * 1000 - event_time_ms > " + LATE_SOURCE_EVENT_THRESHOLD_MS + " THEN 'LATE_SOURCE_EVENT' "
                        + "  ELSE 'NORMAL' "
                        + "END AS anomaly_flag "
                        + "FROM valid_market_ticks"
        );

        StatementSet stmts = tableEnv.createStatementSet();

        /*
         * Insert valid enriched records into Bronze.
         */
        stmts.addInsertSql(
                "INSERT INTO bronze_market_ticks "
                        + "SELECT "
                        + "event_id,"
                        + "event_time,"
                        + "event_time_ms,"
                        + "ingestion_time_ms,"
                        + "source_lag_ms,"
                        + "symbol,"
                        + "price,"
                        + "open_price,"
                        + "high_price,"
                        + "low_price,"
                        + "close_price,"
                        + "volume,"
                        + "source,"
                        + "anomaly_flag,"
                        + "raw_payload,"
                        + "DATE_FORMAT(event_ts, 'yyyy-MM-dd') AS dt "
                        + "FROM enriched_market_ticks"
        );

        /*
         * Insert signals into Kafka.
         */
        stmts.addInsertSql(
                "INSERT INTO market_signals "
                        + "SELECT "
                        + "CONCAT('mkt-', symbol, '-', CAST(event_time_ms AS STRING), '-', CAST(ABS(HASH_CODE(event_id)) AS STRING)) AS signal_id,"
                        + "event_time_ms AS signal_time,"
                        + "symbol,"
                        + "price AS current_price,"
                        + "volume,"
                        + "anomaly_flag,"
                        + "source_lag_ms,"
                        + "CASE "
                        + "  WHEN anomaly_flag IN ('PRICE_ABOVE_HIGH', 'PRICE_BELOW_LOW') THEN 'PRICE_ANOMALY' "
                        + "  WHEN anomaly_flag = 'LATE_SOURCE_EVENT' THEN 'SOURCE_LAG_ALERT' "
                        + "  WHEN volume >= " + HIGH_VOLUME_THRESHOLD + " THEN 'HIGH_VOLUME' "
                        + "  ELSE 'MARKET_TICK' "
                        + "END AS signal_type,"
                        + "CASE "
                        + "  WHEN anomaly_flag IN ('PRICE_ABOVE_HIGH', 'PRICE_BELOW_LOW') THEN 'HIGH' "
                        + "  WHEN anomaly_flag = 'LATE_SOURCE_EVENT' THEN 'MEDIUM' "
                        + "  WHEN volume >= " + HIGH_VOLUME_THRESHOLD + " THEN 'MEDIUM' "
                        + "  ELSE 'LOW' "
                        + "END AS severity,"
                        + "CASE "
                        + "  WHEN anomaly_flag = 'PRICE_ABOVE_HIGH' THEN CONCAT('Price ', CAST(price AS STRING), ' exceeds high ', CAST(high_price AS STRING)) "
                        + "  WHEN anomaly_flag = 'PRICE_BELOW_LOW' THEN CONCAT('Price ', CAST(price AS STRING), ' below low ', CAST(low_price AS STRING)) "
                        + "  WHEN anomaly_flag = 'LATE_SOURCE_EVENT' THEN CONCAT('Source lag is ', CAST(source_lag_ms AS STRING), ' ms') "
                        + "  WHEN volume >= " + HIGH_VOLUME_THRESHOLD + " THEN CONCAT('Volume ', CAST(CAST(volume AS BIGINT) AS STRING), ' crossed high-volume threshold') "
                        + "  ELSE 'Normal market tick' "
                        + "END AS reason,"
                        + "event_id AS source_event_id,"
                        + "source AS raw_event_source "
                        + "FROM enriched_market_ticks"
        );

        /*
         * Insert invalid records into DLQ.
         */
        stmts.addInsertSql(
                "INSERT INTO market_ticks_dlq "
                        + "SELECT "
                        + "event_id,"
                        + "original_event_time,"
                        + "normalized_event_time_ms,"
                        + "symbol,"
                        + "price,"
                        + "open_price,"
                        + "high_price,"
                        + "low_price,"
                        + "close_price,"
                        + "volume,"
                        + "source,"
                        + "validation_error,"
                        + "rejected_at_ms,"
                        + "raw_payload "
                        + "FROM invalid_market_ticks"
        );

        stmts.execute();
    }

    private static String getenv(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}