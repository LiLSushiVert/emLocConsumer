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
                .select(from_json(col("value").cast("string"), schema).alias("data"))
                .select("data.*");

        return stream.writeStream()
                .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("2 seconds"))
                .foreachBatch((batchDF, batchId) -> {
                    if (batchDF.isEmpty()) return;

                    Dataset<Row> enriched = batchDF
                            // Tính momentum_score
                            .withColumn("momentum_score",
                                col("obi_score").multiply(0.4)
                                    .plus(col("buy_active_ratio").multiply(0.3))
                                    .plus(col("price_pos").multiply(0.3)))

                            // Insight Strength (cân bằng, ưu tiên OBI và mua chủ động)
                            .withColumn("insight_strength",
                                col("momentum_score").multiply(50)
                                    .plus(col("obi_score").multiply(25))
                                    .plus(col("buy_active_ratio").multiply(15))
                                    .plus(col("volume").cast("double").divide(1000000).multiply(0.8))
                                    .plus(when(col("change_percent").gt(0),
                                        col("change_percent").multiply(1.2)).otherwise(lit(0))))
                            .withColumn("insight_strength", round(col("insight_strength"), 2))

                            // ==================== LABEL + REASON TỐI ƯU (phiên bản cuối) ====================
                            .withColumn("insight_label",
                                when(col("insight_strength").geq(76)
                                        .and(col("obi_score").geq(0.48))
                                        .and(col("buy_active_ratio").geq(0.68))
                                        .and(col("signal").equalTo("STRONG_BUY")), lit("HOT"))
                                .when(col("insight_strength").geq(55)
                                        .and(col("obi_score").geq(0.36))
                                        .and(col("signal").equalTo("STRONG_BUY")), lit("BUY_SIGNAL"))
                                .when(col("obi_score").geq(0.42).or(
                                        col("buy_active_ratio").geq(0.60)), lit("WATCHLIST"))
                                .otherwise(lit("NORMAL")))

                            .withColumn("reason",
                                when(col("insight_label").equalTo("HOT"),
                                    concat(lit("HOT: OBI cực mạnh ("), round(col("obi_score"), 3),
                                           lit(") + Mua chủ động rất cao + Volume hỗ trợ → Tín hiệu nóng"))
                                )
                                .when(col("insight_label").equalTo("BUY_SIGNAL"),
                                    when(col("liquidity_status").equalTo("HIGH"),
                                        concat(lit("BUY_SIGNAL: OBI tốt ("), round(col("obi_score"), 3),
                                               lit(") + Liquidity cao → Nên theo dõi mua"))
                                    ).otherwise(
                                        concat(lit("BUY_SIGNAL: OBI tốt ("), round(col("obi_score"), 3),
                                               lit(") → Tín hiệu mua tích cực"))
                                    )
                                )
                                .when(col("insight_label").equalTo("WATCHLIST"),
                                    when(col("obi_score").geq(0.5),
                                        concat(lit("WATCHLIST mạnh: OBI rất cao ("), round(col("obi_score"), 3),
                                               lit(") - Đang theo dõi sát để mua"))
                                    ).when(col("buy_active_ratio").geq(0.6),
                                        concat(lit("WATCHLIST mạnh: Mua chủ động cao ("), round(col("buy_active_ratio"), 2),
                                               lit(") - Đáng chú ý"))
                                    ).otherwise(
                                        concat(lit("WATCHLIST: OBI ổn ("), round(col("obi_score"), 3),
                                               lit(") - Đáng theo dõi"))
                                    )
                                )
                                .otherwise(lit("Chưa có tín hiệu rõ ràng, cần theo dõi thêm")))

                            .withColumn("created_at", current_timestamp());

                    // Loại bỏ trùng lặp trong cùng snapshot
                    enriched = enriched.dropDuplicates("window_time", "symbol");

                    // Ghi vào PostgreSQL
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
                            col("reason")
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
                            + " → " + enriched.count() + " rows | "
                            + java.time.LocalDateTime.now());
                })
                .option("checkpointLocation", "D:/spark-checkpoint-job3-final-" + LocalDate.now())
                .start();
    }
}