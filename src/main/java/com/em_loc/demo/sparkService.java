package com.em_loc.demo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import redis.clients.jedis.Jedis;

@Service
public class SparkService {

    private final SparkBuilder sparkBuilder;

    @Value("${postgres.url}")
    private String postgresUrl;

    @Value("${postgres.user}")
    private String postgresUser;

    @Value("${postgres.password}")
    private String postgresPassword;

    private static final String TARGET_TABLE = "market.stock_ticks2";
    private static final String ALERT_TOPIC = "stock-alerts";

    public SparkService(SparkBuilder sparkBuilder) {
        this.sparkBuilder = sparkBuilder;
    }

    public void readKafka() throws Exception {

        SparkSession spark = sparkBuilder.getSparkSession();

        Dataset<Row> kafkaStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9095")
                .option("subscribe", "LOC-VIETCAP-STOCK-DATA-TOPIC")
                .option("startingOffsets", "latest")
                .option("maxOffsetsPerTrigger", "2000")
                .option("failOnDataLoss", "false")
                .load();

        Dataset<Row> messages = kafkaStream.selectExpr(
                "CAST(value AS STRING) as json",
                "timestamp",
                "offset"
        );

        StructType schema = new StructType()
                .add("co", DataTypes.StringType)
                .add("s", DataTypes.StringType)
                .add("orgn", DataTypes.StringType)
                .add("enorgn", DataTypes.StringType)
                .add("st", DataTypes.StringType)
                .add("bo", DataTypes.StringType)
                .add("ref", DataTypes.IntegerType)
                .add("op", DataTypes.IntegerType)
                .add("c", DataTypes.IntegerType)
                .add("h", DataTypes.IntegerType)
                .add("l", DataTypes.IntegerType)
                .add("cei", DataTypes.IntegerType)
                .add("flo", DataTypes.IntegerType)
                .add("avgp", DataTypes.DoubleType)
                .add("vo", DataTypes.LongType)
                .add("va", DataTypes.DoubleType)
                .add("frbv", DataTypes.LongType)
                .add("frsv", DataTypes.LongType)
                .add("frcrr", DataTypes.LongType)
                .add("bp1", DataTypes.StringType)
                .add("bv1", DataTypes.LongType)
                .add("bp2", DataTypes.IntegerType)
                .add("bv2", DataTypes.LongType)
                .add("bp3", DataTypes.IntegerType)
                .add("bv3", DataTypes.LongType)
                .add("ap1", DataTypes.IntegerType)
                .add("av1", DataTypes.LongType)
                .add("ap2", DataTypes.IntegerType)
                .add("av2", DataTypes.LongType)
                .add("ap3", DataTypes.IntegerType)
                .add("av3", DataTypes.LongType)
                .add("ptv", DataTypes.LongType)
                .add("pta", DataTypes.LongType);

        Dataset<Row> parsed = messages
                .select(
                        from_json(col("json"), schema).alias("data"),
                        col("timestamp"),
                        col("offset")
                )
                .select(col("data.*"), col("timestamp"), col("offset"));

        Dataset<Row> transformed = parsed.select(
                col("co").alias("code"),
                col("s").alias("symbol"),
                col("orgn").alias("company_name"),
                col("enorgn").alias("company_name_en"),
                col("bo").alias("exchange"),
                col("st").alias("stock_type"),
                col("ref").alias("ref_price"),
                col("op").alias("open_price"),
                col("c").alias("close_price"),
                col("h").alias("high_price"),
                col("l").alias("low_price"),
                col("flo").alias("floor_price"),
                col("cei").alias("ceiling_price"),
                col("avgp").alias("avg_price"),
                col("vo").alias("volume"),
                col("va").alias("value"),
                col("frbv").alias("foreign_buy_volume"),
                col("frsv").alias("foreign_sell_volume"),
                col("frcrr").alias("foreign_room"),
                col("bp1").cast("int").alias("bid_price_1"),
                col("bv1").alias("bid_volume_1"),
                col("bp2").alias("bid_price_2"),
                col("bv2").alias("bid_volume_2"),
                col("bp3").alias("bid_price_3"),
                col("bv3").alias("bid_volume_3"),
                col("ap1").alias("ask_price_1"),
                col("av1").alias("ask_volume_1"),
                col("ap2").alias("ask_price_2"),
                col("av2").alias("ask_volume_2"),
                col("ap3").alias("ask_price_3"),
                col("av3").alias("ask_volume_3"),
                col("ptv").alias("put_through_volume"),
                col("pta").alias("put_through_value"),
                col("timestamp").alias("created_at"),
                col("offset").alias("offsetkafka")
        );

        StreamingQuery query = transformed.writeStream()
                .foreachBatch((batchDF, batchId) -> {

                    if (batchDF.isEmpty()) return;

                    boolean isDbEmpty = batchDF.sparkSession()
                            .read()
                            .format("jdbc")
                            .option("url", postgresUrl)
                            .option("dbtable", TARGET_TABLE)
                            .option("user", postgresUser)
                            .option("password", postgresPassword)
                            .load()
                            .limit(1)
                            .count() == 0;

                    Dataset<Row> deduped = batchDF
                            .orderBy(col("created_at").desc())
                            .dropDuplicates("symbol");

                    List<Row> rows = deduped.collectAsList();

                    try (Jedis jedis = new Jedis("localhost", 6379)) {

                        // create topic if not exists
                        Properties adminProps = new Properties();
                        adminProps.put("bootstrap.servers", "localhost:9095");

                        try (AdminClient admin = AdminClient.create(adminProps)) {
                            admin.createTopics(
                                    Collections.singleton(new NewTopic(ALERT_TOPIC, 1, (short) 1))
                            );
                        } catch (Exception ignored) {}

                        Properties producerProps = new Properties();
                        producerProps.put("bootstrap.servers", "localhost:9095");
                        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

                        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {

                            List<Row> filtered = new ArrayList<>();
                            double THRESHOLD = 0.001;

                            for (Row row : rows) {

                                String symbol = row.getAs("symbol");
                                Integer price = row.getAs("close_price");

                                if (symbol == null || price == null) continue;

                                String key = "stock:" + symbol;

                                if (isDbEmpty) {
                                    jedis.set(key, price.toString());
                                    filtered.add(row);
                                    continue;
                                }

                                String oldPriceStr = jedis.get(key);

                                if (oldPriceStr == null) {
                                    jedis.set(key, price.toString());
                                    filtered.add(row);
                                    continue;
                                }

                                double oldPrice = Double.parseDouble(oldPriceStr);

                                if (oldPrice == price) continue;

                                double change = Math.abs(price - oldPrice) / oldPrice;

                                if (change > THRESHOLD) {
                                    jedis.set(key, price.toString());
                                    filtered.add(row);
                                }
                            }

                            if (filtered.isEmpty()) return;

                            Dataset<Row> filteredDF = batchDF.sparkSession()
                                    .createDataFrame(filtered, batchDF.schema());

                            Dataset<Row> enrichedDF = filteredDF
                                    .withColumn("change_percent",
                                            round(
                                                    when(col("ref_price").isNotNull().and(col("ref_price").notEqual(0)),
                                                            col("close_price").minus(col("ref_price"))
                                                                    .divide(col("ref_price"))
                                                                    .multiply(100)
                                                    ).otherwise(0), 2)
                                    )
                                    .withColumn("range_percent",
                                            round(
                                                    when(col("ref_price").isNotNull().and(col("ref_price").notEqual(0)),
                                                            col("high_price").minus(col("low_price"))
                                                                    .divide(col("ref_price"))
                                                                    .multiply(100)
                                                    ).otherwise(0), 2)
                                    )
                                    .withColumn("price_direction",
                                            when(col("close_price").gt(col("ref_price")), "UP")
                                                    .when(col("close_price").lt(col("ref_price")), "DOWN")
                                                    .otherwise("FLAT")
                                    )
                                    .withColumn("spread",
                                            col("ask_price_1").minus(col("bid_price_1"))
                                    );

                            enrichedDF.write()
                                    .format("jdbc")
                                    .option("url", postgresUrl)
                                    .option("dbtable", TARGET_TABLE)
                                    .option("user", postgresUser)
                                    .option("password", postgresPassword)
                                    .option("driver", "org.postgresql.Driver")
                                    .option("batchsize", "2000")
                                    .mode("append")
                                    .save();
                        }
                    }

                })
                .outputMode("append")
                .option("checkpointLocation", "D:/spark-checkpoint")
                .start();

        query.awaitTermination();
    }
}