package com.em_loc.demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import static org.apache.spark.sql.functions.*;

@Service
public class SparkService {

    private final SparkBuilder sparkBuilder;

    @Value("${postgres.url}")
    private String postgresUrl;

    @Value("${postgres.user}")
    private String postgresUser;

    @Value("${postgres.password}")
    private String postgresPassword;

    private static final String TARGET_TABLE = "market.stock_ticks";

    public SparkService(SparkBuilder sparkBuilder) {
        this.sparkBuilder = sparkBuilder;
    }

    public void readKafka() throws Exception {

        SparkSession spark = sparkBuilder.getSparkSession();

        // =========================
        // 1. Read Kafka Stream
        // =========================
        Dataset<Row> kafkaStream = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9095")
                .option("subscribe", "LOC-VIETCAP-STOCK-DATA-TOPIC")
                .option("startingOffsets", "latest")
                .option("maxOffsetsPerTrigger", "2000")
                .option("failOnDataLoss", "false")
                .load();

        Dataset<Row> messages = kafkaStream
                .selectExpr("CAST(value AS STRING) as json");

        // =========================
        // 2. JSON Schema
        // =========================
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

        // =========================
        // 3. Parse JSON
        // =========================
        Dataset<Row> parsed = messages
                .select(from_json(col("json"), schema).alias("data"))
                .select("data.*");

        // =========================
        // 4. Transform
        // =========================
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

                current_timestamp().alias("created_at")
        );

        // =========================
        // 5. Write Stream
        // =========================
        StreamingQuery query = transformed.writeStream()

                .foreachBatch((batchDF, batchId) -> {

                    if (batchDF.isEmpty()) {
                        return;
                    }

                    long count = batchDF.count();

                    batchDF.write()
                            .format("jdbc")
                            .option("url", postgresUrl)
                            .option("dbtable", TARGET_TABLE)
                            .option("user", postgresUser)
                            .option("password", postgresPassword)
                            .option("driver", "org.postgresql.Driver")
                            .option("batchsize", "2000")
                            .mode("append")
                            .save();

                    System.out.println("Batch " + batchId + " inserted: " + count);
                })

                .outputMode("append")
                .option("checkpointLocation", "C:/tmp/spark-checkpoint-stock-ticks")
                .start();

        System.out.println("Kafka → Spark → PostgreSQL streaming started");

        query.awaitTermination();
    }
}