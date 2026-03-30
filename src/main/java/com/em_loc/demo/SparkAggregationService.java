package com.em_loc.demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.Serializable;

@Service
public class SparkAggregationService implements Serializable {

    private final SparkBuilder sparkBuilder;

    @Value("${postgres.url}")
    private String postgresUrl;

    @Value("${postgres.user}")
    private String postgresUser;

    @Value("${postgres.password}")
    private String postgresPassword;

    @Value("${kafka.bootstrap-servers:localhost:9095}")
    private String bootstrapServers;

    private static final String SOURCE_TOPIC = "CLEAN-STOCK-DATA";
    private static final String INSIGHT_TABLE = "market.stock_insights";

    public SparkAggregationService(SparkBuilder sparkBuilder) {
        this.sparkBuilder = sparkBuilder;
    }

    public StreamingQuery startAggregation() throws Exception {

        SparkSession spark = sparkBuilder.getSparkSession();

        // Disable schema check (dev only)
        spark.conf().set("spark.sql.streaming.stateStore.stateSchemaCheck", "false");

        // ===== Schema Kafka =====
        StructType schema = new StructType()
                .add("symbol", DataTypes.StringType)
                .add("close_price", DataTypes.IntegerType)
                .add("high_price", DataTypes.IntegerType)
                .add("low_price", DataTypes.IntegerType)
                .add("ceiling_price", DataTypes.IntegerType)
                .add("floor_price", DataTypes.IntegerType)
                .add("volume", DataTypes.LongType)
                .add("bid_volume_1", DataTypes.LongType)
                .add("bid_volume_2", DataTypes.LongType)
                .add("bid_volume_3", DataTypes.LongType)
                .add("ask_volume_1", DataTypes.LongType)
                .add("ask_volume_2", DataTypes.LongType)
                .add("ask_volume_3", DataTypes.LongType)
                .add("spread", DataTypes.IntegerType)
                .add("trade_type", DataTypes.StringType)
                .add("created_at", DataTypes.TimestampType);

        // ===== Read Kafka Stream =====
        Dataset<Row> cleanStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", SOURCE_TOPIC)
                .option("startingOffsets", "earliest")
                .load()
                .select(from_json(col("value").cast("string"), schema).alias("data"))
                .select("data.*");

        // ===== Aggregation =====
        Dataset<Row> insights = cleanStream
                .withWatermark("created_at", "10 minutes")
                .groupBy(
                        window(col("created_at"), "5 minutes", "1 minute"),
                        col("symbol")
                )
                .agg(
                        // OBI Score
                        avg(
                                col("bid_volume_1")
                                        .plus(col("bid_volume_2"))
                                        .plus(col("bid_volume_3"))
                                        .minus(
                                                col("ask_volume_1")
                                                        .plus(col("ask_volume_2"))
                                                        .plus(col("ask_volume_3"))
                                        )
                                        .divide(
                                                col("bid_volume_1")
                                                        .plus(col("bid_volume_2"))
                                                        .plus(col("bid_volume_3"))
                                                        .plus(col("ask_volume_1"))
                                                        .plus(col("ask_volume_2"))
                                                        .plus(col("ask_volume_3"))
                                                        .plus(1)
                                        )
                        ).alias("obi_score"),

                        // Net Volume
                        last("volume").minus(first("volume")).alias("net_volume"),

                        // Support signal logic
                        avg("close_price").alias("avg_close"),
                        last("ceiling_price").alias("ceil")
                )

                // ===== Derived fields (MATCH DB) =====

                // Volume spike
                .withColumn(
                        "is_volume_spike",
                        col("net_volume").gt(100000)
                )

                // Liquidity status
                .withColumn(
                        "liquidity_status",
                        when(col("obi_score").gt(0.2), "HIGH")
                                .when(col("obi_score").lt(-0.2), "LOW")
                                .otherwise("BALANCED")
                )

                // Trading signal
                .withColumn(
                        "signal",
                        when(
                                col("obi_score").gt(0.3)
                                        .and(col("net_volume").gt(50000)),
                                "STRONG_BUY"
                        )
                                .when(
                                        col("ceil").minus(col("avg_close")).lt(100)
                                                .and(col("obi_score").gt(0.2)),
                                        "BREAKOUT_CANDIDATE"
                                )
                                .when(
                                        col("obi_score").lt(-0.3),
                                        "DISTRIBUTION"
                                )
                                .otherwise("NEUTRAL")
                )

                // Window time
                .withColumn("window_time", col("window.end"));

        // ===== Write to PostgreSQL =====
        StreamingQuery query = insights.writeStream()
                .outputMode("update")
                .foreachBatch((batchDF, batchId) -> {

                    Dataset<Row> finalBatch = batchDF
                            .dropDuplicates("symbol", "window_time");

                    finalBatch.select(
                                    col("window_time"),
                                    col("symbol"),
                                    col("obi_score"),
                                    col("net_volume"),
                                    col("is_volume_spike"),
                                    col("liquidity_status"),
                                    col("signal")
                            )
                            .write()
                            .format("jdbc")
                            .option("url", postgresUrl)
                            .option("dbtable", INSIGHT_TABLE)
                            .option("user", postgresUser)
                            .option("password", postgresPassword)
                            .option("driver", "org.postgresql.Driver")
                            .mode("append")
                            .save();

                    System.out.println(
                            "💾 [JOB 2] Batch " + batchId +
                                    " → INSERTED into market.stock_insights"
                    );
                })
                .option("checkpointLocation", "D:/spark-checkpoint-job2")
                .start();

        return query;
    }
}