package com.em_loc.demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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

        // Schema từ Job 2 (đã có change_percent)
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
                    if (batchDF.isEmpty()) return;

                    Dataset<Row> enriched = batchDF
                            // Bảo vệ null cho change_percent
                            .withColumn("change_percent", coalesce(col("change_percent"), lit(0.0)))

                            // ==================== MOMENTUM SCORE ====================
                            .withColumn("momentum_score",
                                col("obi_score").multiply(0.45)
                                    .plus(col("buy_active_ratio").multiply(0.35))
                                    .plus(col("price_pos").multiply(0.20)))

                            // ==================== INSIGHT STRENGTH ====================
                            .withColumn("insight_strength",
                                col("momentum_score").multiply(55)
                                    .plus(col("obi_score").multiply(20))
                                    .plus(col("buy_active_ratio").multiply(12))
                                    .plus(col("volume").cast("double").divide(1000000).multiply(0.4))
                                    .plus(when(col("change_percent").gt(0),
                                            col("change_percent").multiply(1.5))
                                        .otherwise(col("change_percent").multiply(0.8))))
                            .withColumn("insight_strength", round(col("insight_strength"), 2))

                            // ==================== INSIGHT LABEL - CÂN BẰNG BUY & SELL ====================
                            .withColumn("insight_label",
                                // BUY SIDE
                                when(col("insight_strength").geq(78)
                                        .and(col("obi_score").geq(0.50))
                                        .and(col("buy_active_ratio").geq(0.70)), lit("HOT_BUY"))
                                .when(col("insight_strength").geq(60)
                                        .and(col("obi_score").geq(0.38)), lit("BUY_SIGNAL"))
                                .when(col("obi_score").geq(0.42)
                                        .and(col("buy_active_ratio").geq(0.50)), lit("WATCHLIST_BUY"))

                                // SELL SIDE
                                .when(col("insight_strength").leq(22)
                                        .and(col("obi_score").leq(-0.48)), lit("HOT_SELL"))
                                .when(col("insight_strength").leq(35)
                                        .and(col("obi_score").leq(-0.40)), lit("SELL_SIGNAL"))
                                .when(col("obi_score").leq(-0.32), lit("WATCHLIST_SELL"))

                                .otherwise(lit("NEUTRAL")))

                            // ==================== REASON ====================
                            .withColumn("reason",
                                when(col("insight_label").equalTo("HOT_BUY"),
                                    lit("Giá đang rất mạnh + Order book nghiêng mua rõ rệt → Có thể mua theo momentum"))
                                .when(col("insight_label").equalTo("BUY_SIGNAL"),
                                    lit("Có tín hiệu mua tích cực → Nên theo dõi và cân nhắc mua, ưu tiên chờ giá về gần avg_price"))
                                .when(col("insight_label").equalTo("WATCHLIST_BUY"),
                                    lit("Đang có dấu hiệu mua tích cực → Đáng theo dõi để mua"))

                                .when(col("insight_label").equalTo("HOT_SELL"),
                                    lit("Bán mạnh + Order book nghiêng bán rõ → Có thể bán tại giá hiện tại hoặc chờ hồi nhẹ"))
                                .when(col("insight_label").equalTo("SELL_SIGNAL"),
                                    lit("Có dấu hiệu bán mạnh → Nên cân nhắc chốt lời hoặc cắt lỗ"))
                                .when(col("insight_label").equalTo("WATCHLIST_SELL"),
                                    lit("Đang có áp lực bán → Cần theo dõi sát, có thể bán nếu tiếp tục yếu"))

                                .otherwise(lit("Chưa có tín hiệu rõ ràng, nên theo dõi thêm")))

                            .withColumn("created_at", current_timestamp());

                    // Loại bỏ bản ghi trùng
                    enriched = enriched.dropDuplicates("window_time", "symbol");

                    // Ghi vào bảng stock_dashboard (đã có change_percent)
                    enriched.select(
                            col("window_time"),
                            col("created_at"),
                            col("symbol"),
                            col("obi_score"),
                            col("buy_active_ratio"),
                            col("net_volume"),
                            col("money_flow"),
                            col("price_pos"),
                            col("signal"),
                            col("liquidity_status"),
                            col("momentum_score"),
                            col("insight_strength"),
                            col("insight_label"),
                            col("reason"),
                            col("change_percent")          // ← Thêm cột này vì anh đã alter table
                    ).write()
                            .format("jdbc")
                            .option("url", postgresUrl)
                            .option("dbtable", TARGET_TABLE)
                            .option("user", postgresUser)
                            .option("password", postgresPassword)
                            .option("driver", "org.postgresql.Driver")
                            .mode("append")
                            .save();

                    System.out.println("✅ [JOB 3 FINAL] Batch " + batchId 
                            + " → " + enriched.count() + " rows | " + java.time.LocalDateTime.now());
                })
                .option("checkpointLocation", "D:/spark-checkpoint-job3-final-" + LocalDate.now())
                .start();
    }
}