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
                .load();

        // =========================
        // 2. Convert Kafka value -> JSON string
        // =========================
        Dataset<Row> messages = kafkaStream
                .selectExpr("CAST(value AS STRING) as json");

        // =========================
        // 3. Define JSON Schema
        // =========================
        StructType schema = new StructType()
                .add("co", DataTypes.StringType)
                .add("s", DataTypes.StringType)
                .add("c", DataTypes.IntegerType)
                .add("h", DataTypes.IntegerType)
                .add("l", DataTypes.IntegerType)
                .add("op", DataTypes.IntegerType)
                .add("ref", DataTypes.IntegerType)
                .add("avgp", DataTypes.DoubleType)
                .add("vo", DataTypes.LongType)
                .add("va", DataTypes.DoubleType)
                .add("bo", DataTypes.StringType);

        // =========================
        // 4. Parse JSON
        // =========================
        Dataset<Row> parsed = messages
                .select(from_json(col("json"), schema).alias("data"))
                .select("data.*");

        // =========================
        // 5. Transform Data
        // =========================
        Dataset<Row> transformed = parsed

                .withColumnRenamed("co", "code")
                .withColumnRenamed("s", "symbol")
                .withColumnRenamed("c", "close_price")
                .withColumnRenamed("h", "high_price")
                .withColumnRenamed("l", "low_price")
                .withColumnRenamed("op", "open_price")
                .withColumnRenamed("ref", "ref_price")
                .withColumnRenamed("avgp", "avg_price")
                .withColumnRenamed("vo", "volume")
                .withColumnRenamed("va", "value")
                .withColumnRenamed("bo", "exchange")

                // text columns
                .withColumn("company_name", lit(null).cast("string"))
                .withColumn("company_name_en", lit(null).cast("string"))
                .withColumn("stock_type", lit(null).cast("string"))

                // price columns
                .withColumn("floor_price", lit(0).cast("double"))
                .withColumn("ceiling_price", lit(0).cast("double"))

                // foreign trade
                .withColumn("foreign_buy_volume", lit(0L))
                .withColumn("foreign_sell_volume", lit(0L))
                .withColumn("foreign_room", lit(0L))

                // bid
                .withColumn("bid_price_1", lit(0).cast("double"))
                .withColumn("bid_volume_1", lit(0L))
                .withColumn("bid_price_2", lit(0).cast("double"))
                .withColumn("bid_volume_2", lit(0L))
                .withColumn("bid_price_3", lit(0).cast("double"))
                .withColumn("bid_volume_3", lit(0L))

                // ask
                .withColumn("ask_price_1", lit(0).cast("double"))
                .withColumn("ask_volume_1", lit(0L))
                .withColumn("ask_price_2", lit(0).cast("double"))
                .withColumn("ask_volume_2", lit(0L))
                .withColumn("ask_price_3", lit(0).cast("double"))
                .withColumn("ask_volume_3", lit(0L))

                // put-through
                .withColumn("put_through_volume", lit(0L))
                .withColumn("put_through_value", lit(0L));

        // =========================
        // 6. Write Stream → PostgreSQL
        // =========================
        StreamingQuery query = transformed.writeStream()

                .foreachBatch((batchDF, batchId) -> {

                    if (batchDF.isEmpty()) {
                        System.out.println("Batch " + batchId + " empty");
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
                            .option("numPartitions", "4")
                            .mode("append")
                            .save();

                    System.out.println("Batch " + batchId + " inserted: " + count);
                })

                .outputMode("append")
                .option("checkpointLocation", "C:/tmp/spark-checkpoint-stock-ticks")
                .start();

        // =========================
        // 7. Start Streaming
        // =========================
        System.out.println("=================================");
        System.out.println("Kafka → Spark → PostgreSQL streaming started");
        System.out.println("Press Ctrl + C to stop");
        System.out.println("=================================");

        query.awaitTermination();
    }
}