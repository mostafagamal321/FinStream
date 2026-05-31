package com.finstream.flink;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class TransactionScoringJob {

    public static void main(String[] args) throws Exception {
        String kafkaBootstrapServers = getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String schemaRegistryUrl = getEnv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081");
        String inputTopic = getEnv("TOPIC_TRANSACTIONS_RAW", "transactions_raw");
        String consumerGroup = getEnv(
                "FLINK_TRANSACTION_CONSUMER_GROUP",
                "flink-transaction-baseline-v1"
        );

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        configureCheckpointing(env);

        KafkaSource<TransactionEvent> source = KafkaSource.<TransactionEvent>builder()
                .setBootstrapServers(kafkaBootstrapServers)
                .setTopics(inputTopic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest()
                )
                .setDeserializer(new TransactionAvroDeserializationSchema(schemaRegistryUrl))
                .build();

        DataStream<TransactionEvent> transactions = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "transactions_raw_source"
        );

        DataStream<ScoredTransaction> scoredTransactions = transactions
                .map(new BaselineScoringFunction())
                .name("baseline_rule_scoring");

        scoredTransactions
                .print()
                .name("print_scored_transactions");

        scoredTransactions
                .addSink(createClickHouseSink())
                .name("clickhouse_fraud_scores_rt_sink");

        String s3ScoredPath = getEnv("S3_TRANSACTIONS_SCORED_PATH", "");

        if (!s3ScoredPath.isBlank()) {
            scoredTransactions
                    .map(new ScoredTransactionJsonMapper())
                    .name("scored_transaction_to_json")
                    .sinkTo(createS3ScoredTransactionsSink(s3ScoredPath))
                    .name("s3_transactions_scored_sink");

            System.out.println("S3 scored transactions sink enabled. path=" + s3ScoredPath);
        } else {
            System.out.println("S3 scored transactions sink disabled. S3_TRANSACTIONS_SCORED_PATH is empty.");
        }

        env.execute("FinStream Transaction Baseline Scoring Job");
    }

    private static void configureCheckpointing(StreamExecutionEnvironment env) {
        boolean enableCheckpoints = Boolean.parseBoolean(
                getEnv("ENABLE_FLINK_CHECKPOINTS", "false")
        );

        if (!enableCheckpoints) {
            System.out.println("Flink checkpointing disabled for baseline validation.");
            return;
        }

        env.enableCheckpointing(10_000);

        String checkpointDir = getEnv(
                "FLINK_CHECKPOINT_DIR",
                "file:///tmp/finstream-flink-checkpoints"
        );

        env.getCheckpointConfig().setCheckpointStorage(checkpointDir);

        System.out.println("Flink checkpointing enabled. checkpointDir=" + checkpointDir);
    }

    private static String getEnv(String name, String defaultValue) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return value;
    }

    private static SinkFunction<ScoredTransaction> createClickHouseSink() {
        String clickHouseUrl = getEnv(
                "CLICKHOUSE_JDBC_URL",
                "jdbc:clickhouse://clickhouse:8123/finstream"
        );

        String clickHouseUser = getEnv("CLICKHOUSE_USER", "default");
        String clickHousePassword = getEnv("CLICKHOUSE_PASSWORD", "");

        String insertSql = """
                INSERT INTO finstream.fraud_scores_rt
                (
                    event_time,
                    scored_at,
                    transaction_id,
                    customer_id,
                    card_id,
                    merchant_id,
                    amount,
                    currency,
                    product_cd,
                    rule_score,
                    ml_score,
                    fraud_score,
                    scoring_method,
                    risk_level,
                    decision,
                    reason_codes,
                    actual_is_fraud,
                    kafka_topic,
                    kafka_partition,
                    kafka_offset
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;

        return JdbcSink.sink(
                insertSql,
                TransactionScoringJob::bindScoredTransaction,
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(2000)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(clickHouseUrl)
                        .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                        .withUsername(clickHouseUser)
                        .withPassword(clickHousePassword)
                        .build()
        );
    }

    private static void bindScoredTransaction(
            PreparedStatement statement,
            ScoredTransaction scored
    ) throws SQLException {
        statement.setString(1, scored.eventTime);
        statement.setTimestamp(2, Timestamp.from(Instant.parse(scored.scoredAt)));

        statement.setString(3, scored.transactionId);
        statement.setString(4, scored.customerId);
        statement.setString(5, scored.cardId);
        statement.setString(6, scored.merchantId);

        statement.setDouble(7, scored.amount);
        statement.setString(8, scored.currency);

        if (scored.productCd == null) {
            statement.setNull(9, Types.VARCHAR);
        } else {
            statement.setString(9, scored.productCd);
        }

        statement.setDouble(10, scored.ruleScore);

        if (scored.mlScore == null) {
            statement.setNull(11, Types.DOUBLE);
        } else {
            statement.setDouble(11, scored.mlScore);
        }

        statement.setDouble(12, scored.fraudScore);
        statement.setString(13, scored.scoringMethod);
        statement.setString(14, scored.riskLevel);
        statement.setString(15, scored.decision);
        statement.setString(16, String.join(",", scored.reasonCodes));

        if (scored.actualIsFraud == null) {
            statement.setNull(17, Types.INTEGER);
        } else {
            statement.setInt(17, scored.actualIsFraud);
        }

        statement.setString(18, scored.kafkaTopic);
        statement.setInt(19, scored.kafkaPartition);
        statement.setLong(20, scored.kafkaOffset);
    }

    private static FileSink<String> createS3ScoredTransactionsSink(String s3OutputPath) {
        return FileSink
                .forRowFormat(
                        new Path(s3OutputPath),
                        new SimpleStringEncoder<String>("UTF-8")
                )
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd/HH"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(5))
                                .withInactivityInterval(Duration.ofMinutes(2))
                                .withMaxPartSize(MemorySize.ofMebiBytes(128))
                                .build()
                )
                .build();
    }

    public static class ScoredTransactionJsonMapper
            implements MapFunction<ScoredTransaction, String> {

        private transient ObjectMapper objectMapper;

        @Override
        public String map(ScoredTransaction scored) throws Exception {
            if (objectMapper == null) {
                objectMapper = new ObjectMapper();
            }

            return objectMapper.writeValueAsString(scored);
        }
    }

    public static class TransactionAvroDeserializationSchema
            implements KafkaRecordDeserializationSchema<TransactionEvent> {

        private final String schemaRegistryUrl;
        private transient KafkaAvroDeserializer deserializer;

        public TransactionAvroDeserializationSchema(String schemaRegistryUrl) {
            this.schemaRegistryUrl = schemaRegistryUrl;
        }

        @Override
        public void deserialize(
                ConsumerRecord<byte[], byte[]> record,
                Collector<TransactionEvent> out
        ) {
            if (deserializer == null) {
                Properties properties = new Properties();
                properties.put(
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        ByteArrayDeserializer.class.getName()
                );
                properties.put(
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        KafkaAvroDeserializer.class.getName()
                );
                properties.put("schema.registry.url", schemaRegistryUrl);
                properties.put("specific.avro.reader", "false");

                deserializer = new KafkaAvroDeserializer();
                deserializer.configure((java.util.Map) properties, false);
            }

            Object value = deserializer.deserialize(record.topic(), record.value());

            if (!(value instanceof GenericRecord genericRecord)) {
                return;
            }

            TransactionEvent event = TransactionEvent.fromGenericRecord(genericRecord);
            event.kafkaTopic = record.topic();
            event.kafkaPartition = record.partition();
            event.kafkaOffset = record.offset();

            out.collect(event);
        }

        @Override
        public TypeInformation<TransactionEvent> getProducedType() {
            return TypeInformation.of(TransactionEvent.class);
        }
    }

    public static class BaselineScoringFunction
            implements MapFunction<TransactionEvent, ScoredTransaction> {

        @Override
        public ScoredTransaction map(TransactionEvent event) {
            double ruleScore = 0.0;
            List<String> reasonCodes = new ArrayList<>();

            if (event.amount >= 5000.0) {
                ruleScore += 0.40;
                reasonCodes.add("VERY_HIGH_AMOUNT");
            } else if (event.amount >= 1000.0) {
                ruleScore += 0.20;
                reasonCodes.add("HIGH_AMOUNT");
            }

            if (isBlank(event.deviceInfo)) {
                ruleScore += 0.15;
                reasonCodes.add("MISSING_DEVICE_INFO");
            }

            if (isBlank(event.deviceType)) {
                ruleScore += 0.10;
                reasonCodes.add("MISSING_DEVICE_TYPE");
            }

            if ("credit".equalsIgnoreCase(event.cardType)) {
                ruleScore += 0.05;
                reasonCodes.add("CREDIT_CARD");
            }

            if (isBlank(event.payerEmailDomain)) {
                ruleScore += 0.05;
                reasonCodes.add("MISSING_PAYER_EMAIL_DOMAIN");
            }

            if (isBlank(event.receiverEmailDomain)) {
                ruleScore += 0.05;
                reasonCodes.add("MISSING_RECEIVER_EMAIL_DOMAIN");
            }

            ruleScore = Math.min(ruleScore, 1.0);

            String riskLevel;
            String decision;

            if (ruleScore >= 0.75) {
                riskLevel = "HIGH";
                decision = "BLOCK";
            } else if (ruleScore >= 0.40) {
                riskLevel = "MEDIUM";
                decision = "REVIEW";
            } else {
                riskLevel = "LOW";
                decision = "APPROVE";
            }

            ScoredTransaction scored = new ScoredTransaction();

            scored.transactionId = event.transactionId;
            scored.customerId = event.customerId;
            scored.cardId = event.cardId;
            scored.merchantId = event.merchantId;
            scored.eventTime = event.eventTime;
            scored.scoredAt = Instant.now().toString();

            scored.amount = event.amount;
            scored.currency = event.currency;
            scored.productCd = event.productCd;

            scored.ruleScore = ruleScore;
            scored.mlScore = null;
            scored.fraudScore = ruleScore;
            scored.scoringMethod = "RULE_BASELINE";

            scored.riskLevel = riskLevel;
            scored.decision = decision;
            scored.reasonCodes = reasonCodes;

            scored.actualIsFraud = event.isFraud;
            scored.kafkaTopic = event.kafkaTopic;
            scored.kafkaPartition = event.kafkaPartition;
            scored.kafkaOffset = event.kafkaOffset;

            return scored;
        }

        private static boolean isBlank(String value) {
            return value == null || value.isBlank();
        }
    }

    public static class TransactionEvent implements Serializable {
        public String eventId;
        public String transactionId;
        public String eventTime;
        public String ingestionTime;
        public String source;
        public String split;
        public long transactionDt;
        public double amount;
        public String currency;
        public String productCd;
        public String customerId;
        public String cardId;
        public String merchantId;
        public String cardBrand;
        public String cardType;
        public Double addr1;
        public Double addr2;
        public String payerEmailDomain;
        public String receiverEmailDomain;
        public String deviceType;
        public String deviceInfo;
        public Integer isFraud;

        public String kafkaTopic;
        public int kafkaPartition;
        public long kafkaOffset;

        public static TransactionEvent fromGenericRecord(GenericRecord record) {
            TransactionEvent event = new TransactionEvent();

            event.eventId = asString(record, "event_id");
            event.transactionId = asString(record, "transaction_id");
            event.eventTime = asString(record, "event_time");
            event.ingestionTime = asString(record, "ingestion_time");
            event.source = asString(record, "source");
            event.split = asString(record, "split");
            event.transactionDt = asLong(record, "transaction_dt");
            event.amount = asDouble(record, "amount");
            event.currency = asString(record, "currency");
            event.productCd = asString(record, "product_cd");

            event.customerId = asString(record, "customer_id");
            event.cardId = asString(record, "card_id");
            event.merchantId = asString(record, "merchant_id");

            event.cardBrand = asString(record, "card_brand");
            event.cardType = asString(record, "card_type");
            event.addr1 = asNullableDouble(record, "addr1");
            event.addr2 = asNullableDouble(record, "addr2");

            event.payerEmailDomain = asString(record, "payer_email_domain");
            event.receiverEmailDomain = asString(record, "receiver_email_domain");

            event.deviceType = asString(record, "device_type");
            event.deviceInfo = asString(record, "device_info");
            event.isFraud = asNullableInteger(record, "is_fraud");

            return event;
        }

        private static String asString(GenericRecord record, String fieldName) {
            Object value = record.get(fieldName);
            return value == null ? null : value.toString();
        }

        private static long asLong(GenericRecord record, String fieldName) {
            Object value = record.get(fieldName);
            if (value instanceof Number number) {
                return number.longValue();
            }
            return Long.parseLong(value.toString());
        }

        private static double asDouble(GenericRecord record, String fieldName) {
            Object value = record.get(fieldName);
            if (value instanceof Number number) {
                return number.doubleValue();
            }
            return Double.parseDouble(value.toString());
        }

        private static Double asNullableDouble(GenericRecord record, String fieldName) {
            Object value = record.get(fieldName);
            if (value == null) {
                return null;
            }
            if (value instanceof Number number) {
                return number.doubleValue();
            }
            return Double.parseDouble(value.toString());
        }

        private static Integer asNullableInteger(GenericRecord record, String fieldName) {
            Object value = record.get(fieldName);
            if (value == null) {
                return null;
            }
            if (value instanceof Number number) {
                return number.intValue();
            }
            return Integer.parseInt(value.toString());
        }
    }

    public static class ScoredTransaction implements Serializable {
        public String transactionId;
        public String customerId;
        public String cardId;
        public String merchantId;
        public String eventTime;
        public String scoredAt;

        public double amount;
        public String currency;
        public String productCd;

        public double ruleScore;
        public Double mlScore;
        public double fraudScore;
        public String scoringMethod;

        public String riskLevel;
        public String decision;
        public List<String> reasonCodes;

        public Integer actualIsFraud;

        public String kafkaTopic;
        public int kafkaPartition;
        public long kafkaOffset;

        @Override
        public String toString() {
            return "ScoredTransaction{" +
                    "transactionId='" + transactionId + '\'' +
                    ", amount=" + amount +
                    ", ruleScore=" + ruleScore +
                    ", mlScore=" + mlScore +
                    ", fraudScore=" + fraudScore +
                    ", scoringMethod='" + scoringMethod + '\'' +
                    ", riskLevel='" + riskLevel + '\'' +
                    ", decision='" + decision + '\'' +
                    ", reasonCodes=" + reasonCodes +
                    ", actualIsFraud=" + actualIsFraud +
                    ", kafkaPartition=" + kafkaPartition +
                    ", kafkaOffset=" + kafkaOffset +
                    '}';
        }
    }
}