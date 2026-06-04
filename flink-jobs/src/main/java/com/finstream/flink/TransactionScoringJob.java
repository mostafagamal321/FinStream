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
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.core.fs.FSDataInputStream;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
                .setStartingOffsets(getStartingOffsetsInitializer())
                .setDeserializer(new TransactionAvroDeserializationSchema(schemaRegistryUrl))
                .build();

        DataStream<TransactionEvent> transactions = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "transactions_raw_source"
        );

        int dedupTtlDays = Integer.parseInt(getEnv("FLINK_DEDUP_TTL_DAYS", "7"));

        DataStream<TransactionEvent> uniqueTransactions = transactions
                .keyBy(event -> event.transactionId)
                .process(new TransactionDeduplicationFunction(dedupTtlDays))
                .name("deduplicate_by_transaction_id");

        boolean enableMlScorecard = Boolean.parseBoolean(
                getEnv("ENABLE_ML_SCORECARD", "true")
        );

        DataStream<ScoredTransaction> scoredTransactions;

        if (enableMlScorecard) {
            String modelScorecardUri = getEnv(
                    "MODEL_SCORECARD_S3_URI",
                    "s3://finstream-silver-mostafa-dev/ml/models/fraud_scorecard/latest/model_scorecard.json"
            );

            scoredTransactions = uniqueTransactions
                    .map(new ScorecardScoringFunction(modelScorecardUri))
                    .name("redis_feature_scorecard_ml_scoring");

            System.out.println("ML scorecard enabled. modelScorecardUri=" + modelScorecardUri);
        } else {
            scoredTransactions = uniqueTransactions
                    .map(new BaselineScoringFunction())
                    .name("baseline_rule_scoring");

            System.out.println("ML scorecard disabled. Using rule baseline only.");
        }

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
        boolean enableFraudAlertsSink = Boolean.parseBoolean(
        getEnv("ENABLE_FRAUD_ALERTS_SINK", "true")
);

String fraudAlertsTopic = getEnv("TOPIC_FRAUD_ALERTS", "fraud_alerts");

if (enableFraudAlertsSink) {
    scoredTransactions
            .filter(TransactionScoringJob::isFraudAlert)
            .name("filter_high_risk_fraud_alerts")
            .map(new FraudAlertJsonMapper())
            .name("fraud_alert_to_json")
            .sinkTo(createFraudAlertsSink(kafkaBootstrapServers, fraudAlertsTopic))
            .name("kafka_fraud_alerts_sink");

    System.out.println("Kafka fraud alerts sink enabled. topic=" + fraudAlertsTopic);
} else {
    System.out.println("Kafka fraud alerts sink disabled. ENABLE_FRAUD_ALERTS_SINK=false.");
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

    private static OffsetsInitializer getStartingOffsetsInitializer() {
        String startingOffsets = getEnv("FLINK_STARTING_OFFSETS", "latest");

        if ("earliest".equalsIgnoreCase(startingOffsets)) {
            System.out.println("Flink starting offsets: earliest");
            return OffsetsInitializer.earliest();
        }

        System.out.println("Flink starting offsets: latest");
        return OffsetsInitializer.latest();
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

        @Override
        public String map(ScoredTransaction scored) {
            return "{"
                    + "\"transaction_id\":\"" + escapeJson(scored.transactionId) + "\","
                    + "\"customer_id\":\"" + escapeJson(scored.customerId) + "\","
                    + "\"card_id\":\"" + escapeJson(scored.cardId) + "\","
                    + "\"merchant_id\":\"" + escapeJson(scored.merchantId) + "\","
                    + "\"event_time\":\"" + escapeJson(scored.eventTime) + "\","
                    + "\"scored_at\":\"" + escapeJson(scored.scoredAt) + "\","
                    + "\"amount\":" + scored.amount + ","
                    + "\"currency\":\"" + escapeJson(scored.currency) + "\","
                    + "\"product_cd\":\"" + escapeJson(scored.productCd) + "\","
                    + "\"rule_score\":" + scored.ruleScore + ","
                    + "\"ml_score\":" + nullableDouble(scored.mlScore) + ","
                    + "\"fraud_score\":" + scored.fraudScore + ","
                    + "\"scoring_method\":\"" + escapeJson(scored.scoringMethod) + "\","
                    + "\"risk_level\":\"" + escapeJson(scored.riskLevel) + "\","
                    + "\"decision\":\"" + escapeJson(scored.decision) + "\","
                    + "\"reason_codes\":\"" + escapeJson(String.join(",", scored.reasonCodes)) + "\","
                    + "\"actual_is_fraud\":" + nullableInteger(scored.actualIsFraud) + ","
                    + "\"kafka_topic\":\"" + escapeJson(scored.kafkaTopic) + "\","
                    + "\"kafka_partition\":" + scored.kafkaPartition + ","
                    + "\"kafka_offset\":" + scored.kafkaOffset
                    + "}";
        }

        private static String nullableDouble(Double value) {
            return value == null ? "null" : value.toString();
        }

        private static String nullableInteger(Integer value) {
            return value == null ? "null" : value.toString();
        }

        private static String escapeJson(String value) {
            if (value == null) {
                return "";
            }

            StringBuilder escaped = new StringBuilder();

            for (int i = 0; i < value.length(); i++) {
                char c = value.charAt(i);

                switch (c) {
                    case '"':
                        escaped.append("\\\"");
                        break;
                    case '\\':
                        escaped.append("\\\\");
                        break;
                    case '\b':
                        escaped.append("\\b");
                        break;
                    case '\f':
                        escaped.append("\\f");
                        break;
                    case '\n':
                        escaped.append("\\n");
                        break;
                    case '\r':
                        escaped.append("\\r");
                        break;
                    case '\t':
                        escaped.append("\\t");
                        break;
                    default:
                        if (c < 0x20) {
                            escaped.append(String.format("\\u%04x", (int) c));
                        } else {
                            escaped.append(c);
                        }
                }
            }

            return escaped.toString();
        }
    }

    public static class TransactionDeduplicationFunction
            extends KeyedProcessFunction<String, TransactionEvent, TransactionEvent> {

        private final int ttlDays;
        private transient ValueState<Boolean> seenState;

        public TransactionDeduplicationFunction(int ttlDays) {
            this.ttlDays = ttlDays;
        }

        @Override
        public void open(Configuration parameters) {
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.days(ttlDays))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .cleanupFullSnapshot()
                    .build();

            ValueStateDescriptor<Boolean> descriptor =
                    new ValueStateDescriptor<>("seen_transaction_id", Boolean.class);

            descriptor.enableTimeToLive(ttlConfig);

            seenState = getRuntimeContext().getState(descriptor);

            System.out.println("Transaction deduplication enabled. ttlDays=" + ttlDays);
        }

        @Override
        public void processElement(
                TransactionEvent event,
                Context context,
                Collector<TransactionEvent> out
        ) throws Exception {
            if (event.transactionId == null || event.transactionId.isBlank()) {
                return;
            }

            Boolean alreadySeen = seenState.value();

            if (alreadySeen == null || !alreadySeen) {
                seenState.update(true);
                out.collect(event);
            }
        }
    }

private static boolean isFraudAlert(ScoredTransaction scored) {
    return "HIGH".equalsIgnoreCase(scored.riskLevel)
            || "BLOCK".equalsIgnoreCase(scored.decision);
}

private static KafkaSink<String> createFraudAlertsSink(
        String kafkaBootstrapServers,
        String fraudAlertsTopic
) {
    return KafkaSink.<String>builder()
            .setBootstrapServers(kafkaBootstrapServers)
            .setRecordSerializer(
                    KafkaRecordSerializationSchema.builder()
                            .setTopic(fraudAlertsTopic)
                            .setValueSerializationSchema(new SimpleStringSchema())
                            .build()
            )
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
}

public static class FraudAlertJsonMapper
        implements MapFunction<ScoredTransaction, String> {

    @Override
    public String map(ScoredTransaction scored) {
        return "{"
                + "\"alert_type\":\"FRAUD_RISK_ALERT\","
                + "\"transaction_id\":\"" + escapeJson(scored.transactionId) + "\","
                + "\"customer_id\":\"" + escapeJson(scored.customerId) + "\","
                + "\"card_id\":\"" + escapeJson(scored.cardId) + "\","
                + "\"merchant_id\":\"" + escapeJson(scored.merchantId) + "\","
                + "\"event_time\":\"" + escapeJson(scored.eventTime) + "\","
                + "\"scored_at\":\"" + escapeJson(scored.scoredAt) + "\","
                + "\"amount\":" + scored.amount + ","
                + "\"currency\":\"" + escapeJson(scored.currency) + "\","
                + "\"fraud_score\":" + scored.fraudScore + ","
                + "\"risk_level\":\"" + escapeJson(scored.riskLevel) + "\","
                + "\"decision\":\"" + escapeJson(scored.decision) + "\","
                + "\"reason_codes\":\"" + escapeJson(String.join(",", scored.reasonCodes)) + "\","
                + "\"scoring_method\":\"" + escapeJson(scored.scoringMethod) + "\","
                + "\"actual_is_fraud\":" + nullableInteger(scored.actualIsFraud) + ","
                + "\"source_topic\":\"" + escapeJson(scored.kafkaTopic) + "\","
                + "\"source_partition\":" + scored.kafkaPartition + ","
                + "\"source_offset\":" + scored.kafkaOffset
                + "}";
    }

    private static String nullableInteger(Integer value) {
        return value == null ? "null" : value.toString();
    }

    private static String escapeJson(String value) {
        if (value == null) {
            return "";
        }

        StringBuilder escaped = new StringBuilder();

        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);

            switch (c) {
                case '"':
                    escaped.append("\\\"");
                    break;
                case '\\':
                    escaped.append("\\\\");
                    break;
                case '\b':
                    escaped.append("\\b");
                    break;
                case '\f':
                    escaped.append("\\f");
                    break;
                case '\n':
                    escaped.append("\\n");
                    break;
                case '\r':
                    escaped.append("\\r");
                    break;
                case '\t':
                    escaped.append("\\t");
                    break;
                default:
                    if (c < 0x20) {
                        escaped.append(String.format("\\u%04x", (int) c));
                    } else {
                        escaped.append(c);
                    }
            }
        }

        return escaped.toString();
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
            return scoreWithRules(event);
        }

        public static ScoredTransaction scoreWithRules(TransactionEvent event) {
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

    public static class ScorecardScoringFunction
            extends RichMapFunction<TransactionEvent, ScoredTransaction> {

        private final String modelScorecardUri;

        private transient ScorecardModel scorecardModel;
        private transient JedisPool jedisPool;

        public ScorecardScoringFunction(String modelScorecardUri) {
            this.modelScorecardUri = modelScorecardUri;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            this.scorecardModel = ScorecardModel.load(modelScorecardUri);

            String redisHost = getEnv("REDIS_HOST", "redis");
            int redisPort = Integer.parseInt(getEnv("REDIS_PORT", "6379"));
            int redisTimeoutMs = Integer.parseInt(getEnv("REDIS_TIMEOUT_MS", "200"));

            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(8);
            poolConfig.setMaxIdle(4);
            poolConfig.setMinIdle(1);

            this.jedisPool = new JedisPool(
                    poolConfig,
                    redisHost,
                    redisPort,
                    redisTimeoutMs
            );

            try (Jedis jedis = jedisPool.getResource()) {
                jedis.ping();
            }

            System.out.println(
                    "Scorecard model loaded. version="
                            + scorecardModel.modelVersion
                            + ", reviewThreshold="
                            + scorecardModel.reviewThreshold
                            + ", blockThreshold="
                            + scorecardModel.blockThreshold
            );
        }

        @Override
        public void close() {
            if (jedisPool != null) {
                jedisPool.close();
            }
        }

        @Override
        public ScoredTransaction map(TransactionEvent event) {
            ScoredTransaction baseline = BaselineScoringFunction.scoreWithRules(event);

            Map<String, Double> rawFeatures;

            try (Jedis jedis = jedisPool.getResource()) {
                rawFeatures = buildFeatureMap(event, jedis);
            } catch (Exception ex) {
                baseline.mlScore = null;
                baseline.fraudScore = baseline.ruleScore;
                baseline.scoringMethod = "RULE_BASELINE_REDIS_UNAVAILABLE";
                baseline.reasonCodes.add("REDIS_FEATURE_LOOKUP_FAILED");
                return baseline;
            }

            double mlScore = scorecardModel.predict(rawFeatures);
            baseline.mlScore = mlScore;
            baseline.fraudScore = Math.max(baseline.ruleScore, mlScore);
            baseline.scoringMethod = "RULE_PLUS_LOCAL_SCORECARD";

            if (mlScore >= scorecardModel.blockThreshold) {
                baseline.riskLevel = "HIGH";
                baseline.decision = "BLOCK";
                baseline.reasonCodes.add("ML_SCORECARD_BLOCK");
            } else if (mlScore >= scorecardModel.reviewThreshold) {
                if (!"BLOCK".equalsIgnoreCase(baseline.decision)) {
                    baseline.riskLevel = "MEDIUM";
                    baseline.decision = "REVIEW";
                }
                baseline.reasonCodes.add("ML_SCORECARD_REVIEW");
            } else {
                baseline.reasonCodes.add("ML_SCORECARD_LOW_RISK");
            }

            return baseline;
        }

        private Map<String, Double> buildFeatureMap(TransactionEvent event, Jedis jedis) {
            Map<String, Double> features = new LinkedHashMap<>();

            double amount = event.amount;

            double merchantFraudRateSmoothed = redisDouble(
                    jedis, "merchant:fraud_rate_smoothed:" + nullToEmpty(event.merchantId), 0.0);
            double merchantFraudCount = redisDouble(
                    jedis, "merchant:fraud_count:" + nullToEmpty(event.merchantId), 0.0);
            double merchantTxnCount = redisDouble(
                    jedis, "merchant:txn_count:" + nullToEmpty(event.merchantId), 0.0);
            double merchantAvgAmount = redisDouble(
                    jedis, "merchant:avg_amount:" + nullToEmpty(event.merchantId), 0.0);

            double customerFraudRateSmoothed = redisDouble(
                    jedis, "customer:fraud_rate_smoothed:" + nullToEmpty(event.customerId), 0.0);
            double customerFraudCount = redisDouble(
                    jedis, "customer:fraud_count:" + nullToEmpty(event.customerId), 0.0);
            double customerTxnCount = redisDouble(
                    jedis, "customer:txn_count:" + nullToEmpty(event.customerId), 0.0);
            double customerAvgAmount = redisDouble(
                    jedis, "customer:avg_amount:" + nullToEmpty(event.customerId), 0.0);

            double cardFraudRateSmoothed = redisDouble(
                    jedis, "card:fraud_rate_smoothed:" + nullToEmpty(event.cardId), 0.0);
            double cardFraudCount = redisDouble(
                    jedis, "card:fraud_count:" + nullToEmpty(event.cardId), 0.0);
            double cardTxnCount = redisDouble(
                    jedis, "card:txn_count:" + nullToEmpty(event.cardId), 0.0);
            double cardAvgAmount = redisDouble(
                    jedis, "card:avg_amount:" + nullToEmpty(event.cardId), 0.0);

            features.put("amount", amount);
            features.put("merchant_fraud_rate_smoothed", merchantFraudRateSmoothed);
            features.put("merchant_fraud_count", merchantFraudCount);
            features.put("merchant_txn_count", merchantTxnCount);
            features.put("merchant_avg_amount", merchantAvgAmount);
            features.put("customer_fraud_rate_smoothed", customerFraudRateSmoothed);
            features.put("customer_fraud_count", customerFraudCount);
            features.put("customer_txn_count", customerTxnCount);
            features.put("customer_avg_amount", customerAvgAmount);
            features.put("card_fraud_rate_smoothed", cardFraudRateSmoothed);
            features.put("card_fraud_count", cardFraudCount);
            features.put("card_txn_count", cardTxnCount);
            features.put("card_avg_amount", cardAvgAmount);
            features.put("amount_to_customer_avg", safeRatio(amount, customerAvgAmount));
            features.put("amount_to_card_avg", safeRatio(amount, cardAvgAmount));
            features.put("amount_to_merchant_avg", safeRatio(amount, merchantAvgAmount));

            return features;
        }

        private static double redisDouble(Jedis jedis, String key, double defaultValue) {
            String value = jedis.get(key);
            if (value == null || value.isBlank()) {
                return defaultValue;
            }
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException ex) {
                return defaultValue;
            }
        }

        private static double safeRatio(double numerator, double denominator) {
            if (denominator <= 0.0) return 0.0;
            return numerator / denominator;
        }

        private static String nullToEmpty(String value) {
            return value == null ? "" : value;
        }
    }

    public static class ScorecardModel implements Serializable {
        public String modelVersion;
        public List<String> featureColumns;
        public Map<String, Double> weights;
        public Map<String, Double> imputerMedians;
        public Map<String, Double> scalerMeans;
        public Map<String, Double> scalerScales;
        public double intercept;
        public double reviewThreshold;
        public double blockThreshold;

        public static ScorecardModel load(String modelUri) throws Exception {
            String json = readTextFromPath(modelUri);
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(json);

            ScorecardModel model = new ScorecardModel();
            model.modelVersion = root.path("model_version").asText("unknown");
            model.intercept = root.path("intercept").asDouble(0.0);
            model.reviewThreshold = root.path("review_threshold").asDouble(0.30);
            model.blockThreshold = root.path("block_threshold").asDouble(0.70);

            model.featureColumns = new ArrayList<>();
            for (JsonNode feature : root.path("feature_columns")) {
                model.featureColumns.add(feature.asText());
            }

            model.weights = parseDoubleMap(root.path("weights"));
            model.imputerMedians = parseDoubleMap(root.path("imputer_medians"));
            model.scalerMeans = parseDoubleMap(root.path("scaler_means"));
            model.scalerScales = parseDoubleMap(root.path("scaler_scales"));

            if (model.featureColumns.isEmpty()) {
                throw new IllegalStateException("Scorecard model has no feature_columns");
            }

            return model;
        }

        public double predict(Map<String, Double> rawFeatures) {
            double z = intercept;

            for (String featureName : featureColumns) {
                double rawValue = rawFeatures.getOrDefault(
                        featureName,
                        imputerMedians.getOrDefault(featureName, 0.0)
                );

                if (Double.isNaN(rawValue) || Double.isInfinite(rawValue)) {
                    rawValue = imputerMedians.getOrDefault(featureName, 0.0);
                }

                double mean = scalerMeans.getOrDefault(featureName, 0.0);
                double scale = scalerScales.getOrDefault(featureName, 1.0);

                double scaledValue;
                if (scale == 0.0 || Double.isNaN(scale) || Double.isInfinite(scale)) {
                    scaledValue = 0.0;
                } else {
                    scaledValue = (rawValue - mean) / scale;
                }

                z += weights.getOrDefault(featureName, 0.0) * scaledValue;
            }

            return sigmoid(z);
        }

        private static double sigmoid(double z) {
            if (z >= 0) {
                return 1.0 / (1.0 + Math.exp(-z));
            }
            double exp = Math.exp(z);
            return exp / (1.0 + exp);
        }

        private static Map<String, Double> parseDoubleMap(JsonNode node) {
            Map<String, Double> result = new LinkedHashMap<>();
            node.fields().forEachRemaining(e -> result.put(e.getKey(), e.getValue().asDouble(0.0)));
            return result;
        }

        private static String readTextFromPath(String uri) throws Exception {
            Path path = new Path(uri);
            try (FSDataInputStream inputStream = path.getFileSystem().open(path);
                 ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
                byte[] data = new byte[8192];
                int bytesRead;
                while ((bytesRead = inputStream.read(data)) != -1) {
                    buffer.write(data, 0, bytesRead);
                }
                return buffer.toString(StandardCharsets.UTF_8);
            }
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