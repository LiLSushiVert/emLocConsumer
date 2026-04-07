package com.em_loc.demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDate;

@Service
public class SparkRankingService {

    private final SparkBuilder sparkBuilder;

    @Value("${postgres.url}") private String postgresUrl;
    @Value("${postgres.user}") private String postgresUser;
    @Value("${postgres.password}") private String postgresPassword;
    @Value("${kafka.bootstrap-servers:localhost:9095}") private String bootstrapServers;

    private static final String SOURCE_TOPIC = "STOCK-INSIGHTS-TOPIC";
    private static final String TARGET_TABLE = "market.stock_dashboard";

    public SparkRankingService(SparkBuilder sparkBuilder) {
        this.sparkBuilder = sparkBuilder;
    }

    public StreamingQuery startRankingJob() throws Exception {
        SparkSession spark = sparkBuilder.getSparkSession();

        StructType schema = new StructType()
                .add("window_time", "timestamp")
                .add("symbol", "string")
                .add("close_price", "integer")
                .add("avg_price", "double")
                .add("volume", "long")
                .add("total_bid", "long")
                .add("total_ask", "long")
                .add("obi_score", "double")
                .add("liquidity_status", "string")
                .add("signal", "string")
                .add("buy_active_ratio", "double")
                .add("money_flow", "double")
                .add("price_pos", "double")
                .add("net_volume", "long")
                .add("change_percent", "double");

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
                .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("2 seconds"))
                .foreachBatch((batchDF, batchId) -> {

                    if (batchDF.isEmpty()) {
                        System.out.println("⚪ [JOB 3] Batch " + batchId + " empty");
                        return;
                    }

                    // Dedup latest per symbol
                    Dataset<Row> deduped = batchDF
                            .withColumn("rn", row_number()
                                    .over(Window.partitionBy("symbol")
                                            .orderBy(col("window_time").desc())))
                            .where(col("rn").equalTo(1))
                            .drop("rn");

                    Dataset<Row> enriched = deduped
                            .withColumn("change_percent", coalesce(col("change_percent"), lit(0.0)))

                            // Normalize buy_active_ratio
                            .withColumn("buy_active_ratio_norm",
                                    col("buy_active_ratio").multiply(2).minus(1))

                            // price_pos
                            .withColumn("price_pos",
                                    when(col("close_price").gt(col("avg_price")), lit(1.0))
                                            .otherwise(lit(0.0)))

                            // Momentum score
                            .withColumn("momentum_score",
                                    col("obi_score").multiply(0.6)
                                            .plus(col("buy_active_ratio_norm").multiply(0.3))
                                            .plus(col("price_pos").multiply(0.1)))

                            // Insight strength
                            .withColumn("insight_strength",
                                    round(
                                        col("momentum_score").multiply(100)
                                            .plus(col("volume").cast("double").divide(1000000).multiply(0.5))
                                            .plus(col("change_percent").multiply(1.2))
                                    , 2))

                            // Insight label
                            .withColumn("insight_label",
                                    when(col("obi_score").geq(0.7)
                                            .and(col("momentum_score").geq(0.6)), lit("HOT_BUY"))
                                    .when(col("obi_score").geq(0.5)
                                            .and(col("insight_strength").geq(50)), lit("BUY_SIGNAL"))
                                    .when(col("obi_score").geq(0.35)
                                            .and(col("momentum_score").geq(0.45)), lit("WATCHLIST_BUY"))
                                    .when(col("obi_score").leq(-0.7)
                                            .and(col("momentum_score").leq(-0.6)), lit("HOT_SELL"))
                                    .when(col("obi_score").leq(-0.5)
                                            .and(col("insight_strength").leq(40)), lit("SELL_SIGNAL"))
                                    .when(col("obi_score").leq(-0.35)
                                            .and(col("momentum_score").leq(-0.45)), lit("WATCHLIST_SELL"))
                                    .otherwise(lit("NEUTRAL")))

                            // Reason
                            .withColumn("reason",
                                    when(col("insight_label").equalTo("HOT_BUY"),
                                            lit("Order book mua mạnh + momentum xác nhận"))
                                    .when(col("insight_label").equalTo("BUY_SIGNAL"),
                                            lit("Tín hiệu mua tích cực"))
                                    .when(col("insight_label").equalTo("WATCHLIST_BUY"),
                                            lit("Đang có dấu hiệu mua"))
                                    .when(col("insight_label").equalTo("HOT_SELL"),
                                            lit("Order book bán mạnh + momentum xác nhận"))
                                    .when(col("insight_label").equalTo("SELL_SIGNAL"),
                                            lit("Tín hiệu bán mạnh"))
                                    .when(col("insight_label").equalTo("WATCHLIST_SELL"),
                                            lit("Áp lực bán tăng"))
                                    .otherwise(lit("Chưa rõ xu hướng")))

                            // Final action
                            .withColumn("final_action",
                                    when(col("obi_score").gt(0.65)
                                            .and(col("change_percent").gt(0))
                                            .and(col("price_pos").equalTo(1.0)), lit("MOMENTUM_BUY"))
                                    .when(col("obi_score").gt(0.65)
                                            .and(col("change_percent").lt(0)), lit("ABSORPTION_BUY"))
                                    .when(col("obi_score").lt(-0.65)
                                            .and(col("change_percent").gt(0)), lit("DISTRIBUTION_SELL"))
                                    .when(col("obi_score").lt(-0.65)
                                            .and(col("change_percent").lt(0))
                                            .and(col("price_pos").equalTo(0.0)), lit("MOMENTUM_SELL"))
                                    .otherwise(lit("NEUTRAL")))

                            // Contextual reason
                            .withColumn("contextual_reason",
                                    when(col("final_action").equalTo("MOMENTUM_BUY"),
                                            lit("Mua mạnh + giá tăng → momentum"))
                                    .when(col("final_action").equalTo("ABSORPTION_BUY"),
                                            lit("Mua mạnh nhưng giá giảm → hấp thụ"))
                                    .when(col("final_action").equalTo("DISTRIBUTION_SELL"),
                                            lit("Bán mạnh nhưng giá tăng → phân phối"))
                                    .when(col("final_action").equalTo("MOMENTUM_SELL"),
                                            lit("Bán mạnh + giá giảm → xu hướng giảm"))
                                    .otherwise(lit("Chưa rõ tín hiệu")))

                            .withColumn("created_at", current_timestamp());

                    // ==================== WRITE TO POSTGRES (KHÔNG CÒN count() nữa) ====================
                    enriched.select(
                            col("window_time"), col("created_at"), col("symbol"),
                            col("obi_score"), col("buy_active_ratio"), col("net_volume"),
                            col("money_flow"), col("price_pos"), col("signal"),
                            col("liquidity_status"), col("momentum_score"),
                            col("insight_strength"), col("insight_label"), col("reason"),
                            col("change_percent"), col("final_action"), col("contextual_reason")
                    ).write()
                            .format("jdbc")
                            .option("url", postgresUrl)
                            .option("dbtable", TARGET_TABLE)
                            .option("user", postgresUser)
                            .option("password", postgresPassword)
                            .option("driver", "org.postgresql.Driver")
                            .mode("append")
                            .save();

                    System.out.println("✅ [JOB 3] Batch " + batchId + " → " + enriched.count() 
                            + " rows written to DB | " + java.time.LocalDateTime.now());

                })
                .option("checkpointLocation", "D:/spark-checkpoint-job3-final-" + LocalDate.now())
                .start();
    }
}