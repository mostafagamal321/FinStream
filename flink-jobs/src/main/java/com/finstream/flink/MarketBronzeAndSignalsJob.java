package com.finstream.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MarketBronzeAndSignalsJob {

    public static void main(String[] args) {
        String kafkaBootstrapServers = getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String schemaRegistryUrl = getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081");

        String inputTopic = getenv("TOPIC_MARKET_TICKS_RAW", "market_ticks_raw");
        String outputTopic = getenv("TOPIC_MARKET_SIGNALS", "market_signals");

        String s3Bucket = getenv(
                "FINSTREAM_BRONZE_BUCKET",
                getenv("S3_BUCKET", "finstream-bronze-mostafa-dev")
        );

        String s3MarketPath = "s3a://" + s3Bucket + "/bronze/market_ticks/";

        System.out.println("Kafka bootstrap servers: " + kafkaBootstrapServers);
        System.out.println("Schema Registry URL: " + schemaRegistryUrl);
        System.out.println("Input topic: " + inputTopic);
        System.out.println("Output topic: " + outputTopic);
        System.out.println("Using S3 bronze bucket: " + s3Bucket);
        System.out.println("Using S3 market path: " + s3MarketPath);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(30000);
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.getConfig().getConfiguration().setString(
                "execution.checkpointing.interval",
                "30 s"
        );

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
                        + "event_ts AS TO_TIMESTAMP_LTZ(event_time, 3),"
                        + "WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND"
                        + ") WITH ("
                        + "'connector' = 'kafka',"
                        + "'topic' = '" + inputTopic + "',"
                        + "'properties.bootstrap.servers' = '" + kafkaBootstrapServers + "',"
                        + "'properties.group.id' = 'flink-market-bronze-signals-job',"
                        + "'scan.startup.mode' = 'latest-offset',"
                        + "'format' = 'avro-confluent',"
                        + "'avro-confluent.schema-registry.url' = '" + schemaRegistryUrl + "'"
                        + ")"
        );

        tableEnv.executeSql(
                "CREATE TABLE bronze_market_ticks ("
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
                        + "dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "'connector' = 'filesystem',"
                        + "'path' = '" + s3MarketPath + "',"
                        + "'format' = 'parquet',"
                        + "'sink.partition-commit.policy.kind' = 'success-file',"
                        + "'sink.partition-commit.delay' = '1 min'"
                        + ")"
        );

        tableEnv.executeSql(
                "CREATE TABLE market_signals ("
                        + "signal_id STRING,"
                        + "signal_time BIGINT,"
                        + "symbol STRING,"
                        + "current_price DOUBLE,"
                        + "volume DOUBLE,"
                        + "signal_type STRING,"
                        + "severity STRING,"
                        + "reason STRING,"
                        + "source_event_id STRING,"
                        + "raw_event_source STRING"
                        + ") WITH ("
                        + "'connector' = 'kafka',"
                        + "'topic' = '" + outputTopic + "',"
                        + "'properties.bootstrap.servers' = '" + kafkaBootstrapServers + "',"
                        + "'format' = 'json'"
                        + ")"
        );

        StatementSet statementSet = tableEnv.createStatementSet();

        statementSet.addInsertSql(
                "INSERT INTO bronze_market_ticks "
                        + "SELECT "
                        + "event_id,"
                        + "event_time,"
                        + "symbol,"
                        + "price,"
                        + "open_price,"
                        + "high_price,"
                        + "low_price,"
                        + "close_price,"
                        + "volume,"
                        + "source,"
                        + "raw_payload,"
                        + "DATE_FORMAT(event_ts, 'yyyy-MM-dd') AS dt "
                        + "FROM market_ticks_raw"
        );

        statementSet.addInsertSql(
                "INSERT INTO market_signals "
                        + "SELECT "
                        + "CONCAT('market-', symbol, '-', CAST(event_time AS STRING)) AS signal_id,"
                        + "event_time AS signal_time,"
                        + "symbol,"
                        + "price AS current_price,"
                        + "volume,"
                        + "CASE "
                        + "  WHEN volume IS NOT NULL AND volume >= 1000000 THEN 'HIGH_VOLUME' "
                        + "  ELSE 'MARKET_TICK' "
                        + "END AS signal_type,"
                        + "CASE "
                        + "  WHEN volume IS NOT NULL AND volume >= 1000000 THEN 'MEDIUM' "
                        + "  ELSE 'LOW' "
                        + "END AS severity,"
                        + "CASE "
                        + "  WHEN volume IS NOT NULL AND volume >= 1000000 THEN 'Volume crossed high-volume threshold' "
                        + "  ELSE 'Normal market tick received' "
                        + "END AS reason,"
                        + "event_id AS source_event_id,"
                        + "source AS raw_event_source "
                        + "FROM market_ticks_raw"
        );

        statementSet.execute();
    }

    private static String getenv(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}