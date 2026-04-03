package com.em_loc.demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDate;

@Service
public class SparkAggregationService {

    private final SparkBuilder sparkBuilder;

    @Value("${postgres.url}") private String postgresUrl;
    @Value("${postgres.user}") private String postgresUser;
    @Value("${postgres.password}") private String postgresPassword;
    @Value("${kafka.bootstrap-servers:localhost:9095}") private String bootstrapServers;

    private static final String SOURCE_TOPIC = "CLEAN-STOCK-DATA";
    private static final String INSIGHT_TABLE = "market.stock_insights";
    private static final String INSIGHTS_TOPIC = "STOCK-INSIGHTS-TOPIC";

    public SparkAggregationService(SparkBuilder sparkBuilder) {
        this.sparkBuilder = sparkBuilder;
    }

    public StreamingQuery startAggregation() throws Exception {
        SparkSession spark = sparkBuilder.getSparkSession();

        // Schema đầy đủ hơn, thêm change_percent
        StructType schema = new StructType()
                .add("symbol", "string")
                .add("close_price", "integer")
                .add("avg_price", "double")
                .add("volume", "long")
                .add("bid_volume_1", "long")
                .add("bid_volume_2", "long")
                .add("bid_volume_3", "long")
                .add("ask_volume_1", "long")
                .add("ask_volume_2", "long")
                .add("ask_volume_3", "long")
                .add("trade_type", "string")
                .add("change_percent", "double")
                .add("created_at", "timestamp");

        Dataset<Row> stream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", SOURCE_TOPIC)
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .load()
                .select(from_json(col("value").cast("string"), schema, 
                        java.util.Collections.singletonMap("allowUnmatched", "true"))
                        .alias("data"))
                .select("data.*");

        return stream.writeStream()
                .trigger(Trigger.ProcessingTime("2 seconds"))
                .foreachBatch((batchDF, batchId) -> {
                    if (batchDF.isEmpty()) {
                        System.out.println("⚪ [JOB 2] Batch " + batchId + " empty");
                        return;
                    }

                    Dataset<Row> latest = batchDF
                            .orderBy(col("created_at").desc())
                            .dropDuplicates("symbol");

                    Dataset<Row> insights = latest
                            .withColumn("total_bid", col("bid_volume_1").plus(col("bid_volume_2")).plus(col("bid_volume_3")))
                            .withColumn("total_ask", col("ask_volume_1").plus(col("ask_volume_2")).plus(col("ask_volume_3")))
                            .withColumn("obi_score",
                                when(col("total_bid").plus(col("total_ask")).gt(0),
                                    col("total_bid").minus(col("total_ask"))
                                        .divide(col("total_bid").plus(col("total_ask")).plus(10))
                                ).otherwise(lit(0.0)))
                            .withColumn("liquidity_status",
                                when(col("obi_score").gt(0.25), "HIGH")
                                .when(col("obi_score").lt(-0.25), "LOW")
                                .otherwise("BALANCED"))
                            .withColumn("signal",
                                when(col("obi_score").gt(0.35), "STRONG_BUY")
                                .when(col("obi_score").lt(-0.35), "DISTRIBUTION")
                                .otherwise("NEUTRAL"))
                            .withColumn("buy_active_ratio",
                                when(col("trade_type").equalTo("BUY_ACTIVE"), lit(1.0))
                                .otherwise(lit(0.0)))
                            .withColumn("money_flow", col("avg_price").multiply(col("volume").cast("double")))
                            .withColumn("price_pos",
                                when(col("close_price").gt(col("avg_price")), lit(0.7))
                                .when(col("close_price").lt(col("avg_price")), lit(0.3))
                                .otherwise(lit(0.5)))
                            .withColumn("net_volume", col("volume"))
                            .select(
                                col("created_at").alias("window_time"),
                                col("symbol"),
                                col("close_price"),
                                col("avg_price"),
                                col("volume"),
                                col("total_bid"),
                                col("total_ask"),
                                col("obi_score"),
                                col("liquidity_status"),
                                col("signal"),
                                col("buy_active_ratio"),
                                col("money_flow"),
                                col("price_pos"),
                                col("net_volume"),
                                coalesce(col("change_percent"), lit(0.0)).alias("change_percent")   // ← Bảo vệ null
                            );

                    // Ghi Postgres
                    insights.write()
                            .format("jdbc")
                            .option("url", postgresUrl)
                            .option("dbtable", INSIGHT_TABLE)
                            .option("user", postgresUser)
                            .option("password", postgresPassword)
                            .option("driver", "org.postgresql.Driver")
                            .mode("append")
                            .save();

                    // Push Kafka cho Job 3
                    insights.select(to_json(struct(col("*"))).alias("value"))
                            .write()
                            .format("kafka")
                            .option("kafka.bootstrap.servers", bootstrapServers)
                            .option("topic", INSIGHTS_TOPIC)
                            .save();

                    System.out.println("✅ [JOB 2] Batch " + batchId + " → " + insights.count() 
                            + " symbols → Postgres + Kafka | " + java.time.LocalDateTime.now());
                })
                .option("checkpointLocation", "D:/spark-checkpoint-job2-" + LocalDate.now())
                .start();
    }
}