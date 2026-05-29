package com.finstream.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.StatementSet;

public class NewsBronzeAndSignalsJob {

    public static void main(String[] args) {
        String kafkaBootstrapServers = getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String schemaRegistryUrl = getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081");

        String inputTopic = getenv("TOPIC_NEWS_RAW", "news_raw");
        String outputTopic = getenv("TOPIC_NEWS_SIGNALS", "news_signals");

        String s3Bucket = getenv(
                "FINSTREAM_BRONZE_BUCKET",
                getenv("S3_BUCKET", "finstream-bronze-mostafa-dev")
        );

        String s3NewsPath = "s3a://" + s3Bucket + "/bronze/market_news/";

        System.out.println("Kafka bootstrap servers: " + kafkaBootstrapServers);
        System.out.println("Schema Registry URL: " + schemaRegistryUrl);
        System.out.println("Input topic: " + inputTopic);
        System.out.println("Output topic: " + outputTopic);
        System.out.println("Using S3 bronze bucket: " + s3Bucket);
        System.out.println("Using S3 news path: " + s3NewsPath);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30000);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

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
                        + "event_ts AS TO_TIMESTAMP_LTZ(event_time, 3),"
                        + "WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND"
                        + ") WITH ("
                        + "'connector' = 'kafka',"
                        + "'topic' = '" + inputTopic + "',"
                        + "'properties.bootstrap.servers' = '" + kafkaBootstrapServers + "',"
                        + "'properties.group.id' = 'flink-news-bronze-signals-job',"
                        + "'scan.startup.mode' = 'earliest-offset',"
                        + "'format' = 'avro-confluent',"
                        + "'avro-confluent.schema-registry.url' = '" + schemaRegistryUrl + "'"
                        + ")"
        );

        tableEnv.executeSql(
                "CREATE TABLE bronze_news ("
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
                        + "dt STRING"
                        + ") PARTITIONED BY (dt) WITH ("
                        + "'connector' = 'filesystem',"
                        + "'path' = '" + s3NewsPath + "',"
                        + "'format' = 'parquet',"
                        + "'sink.partition-commit.policy.kind' = 'success-file',"
                        + "'sink.partition-commit.delay' = '1 min'"
                        + ")"
        );

        tableEnv.executeSql(
                "CREATE TABLE news_signals ("
                        + "signal_id STRING,"
                        + "signal_time BIGINT,"
                        + "title STRING,"
                        + "source_name STRING,"
                        + "published_at STRING,"
                        + "symbol_query STRING,"
                        + "sentiment_label STRING,"
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
                "INSERT INTO bronze_news "
                        + "SELECT "
                        + "event_id,"
                        + "event_time,"
                        + "published_at,"
                        + "source_name,"
                        + "author,"
                        + "title,"
                        + "description,"
                        + "url,"
                        + "symbol_query,"
                        + "source,"
                        + "raw_payload,"
                        + "DATE_FORMAT(event_ts, 'yyyy-MM-dd') AS dt "
                        + "FROM news_raw"
        );

        statementSet.addInsertSql(
                "INSERT INTO news_signals "
                        + "SELECT "
                        + "CONCAT('news-', CAST(event_time AS STRING), '-', event_id) AS signal_id,"
                        + "event_time AS signal_time,"
                        + "title,"
                        + "source_name,"
                        + "published_at,"
                        + "symbol_query,"
                        + "CASE "
                        + "  WHEN LOWER(CONCAT(title, ' ', COALESCE(description, ''))) LIKE '%fraud%' "
                        + "    OR LOWER(CONCAT(title, ' ', COALESCE(description, ''))) LIKE '%lawsuit%' "
                        + "    OR LOWER(CONCAT(title, ' ', COALESCE(description, ''))) LIKE '%investigation%' "
                        + "    OR LOWER(CONCAT(title, ' ', COALESCE(description, ''))) LIKE '%crash%' "
                        + "    OR LOWER(CONCAT(title, ' ', COALESCE(description, ''))) LIKE '%recession%' "
                        + "  THEN 'NEGATIVE' "
                        + "  WHEN LOWER(CONCAT(title, ' ', COALESCE(description, ''))) LIKE '%growth%' "
                        + "    OR LOWER(CONCAT(title, ' ', COALESCE(description, ''))) LIKE '%profit%' "
                        + "    OR LOWER(CONCAT(title, ' ', COALESCE(description, ''))) LIKE '%rally%' "
                        + "    OR LOWER(CONCAT(title, ' ', COALESCE(description, ''))) LIKE '%surge%' "
                        + "  THEN 'POSITIVE' "
                        + "  ELSE 'NEUTRAL' "
                        + "END AS sentiment_label,"
                        + "CASE "
                        + "  WHEN LOWER(CONCAT(title, ' ', COALESCE(description, ''))) LIKE '%fraud%' "
                        + "    OR LOWER(CONCAT(title, ' ', COALESCE(description, ''))) LIKE '%lawsuit%' "
                        + "    OR LOWER(CONCAT(title, ' ', COALESCE(description, ''))) LIKE '%investigation%' "
                        + "    OR LOWER(CONCAT(title, ' ', COALESCE(description, ''))) LIKE '%crash%' "
                        + "    OR LOWER(CONCAT(title, ' ', COALESCE(description, ''))) LIKE '%recession%' "
                        + "  THEN 'NEGATIVE_NEWS_SIGNAL' "
                        + "  ELSE 'NEWS_MENTION' "
                        + "END AS signal_type,"
                        + "CASE "
                        + "  WHEN LOWER(CONCAT(title, ' ', COALESCE(description, ''))) LIKE '%fraud%' "
                        + "    OR LOWER(CONCAT(title, ' ', COALESCE(description, ''))) LIKE '%lawsuit%' "
                        + "    OR LOWER(CONCAT(title, ' ', COALESCE(description, ''))) LIKE '%investigation%' "
                        + "  THEN 'HIGH' "
                        + "  ELSE 'LOW' "
                        + "END AS severity,"
                        + "'Keyword-based news signal generated by Flink' AS reason,"
                        + "event_id AS source_event_id,"
                        + "source AS raw_event_source "
                        + "FROM news_raw"
        );

        statementSet.execute();
    }

    private static String getenv(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}