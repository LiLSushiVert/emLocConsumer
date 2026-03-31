package com.em_loc.demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import static org.apache.spark.sql.functions.*;

import java.sql.Timestamp;

@Service
public class SparkRankingService {

    private final SparkBuilder sparkBuilder;

    @Value("${postgres.url}") private String postgresUrl;
    @Value("${postgres.user}") private String postgresUser;
    @Value("${postgres.password}") private String postgresPassword;

    private static final String SOURCE_TABLE = "market.stock_insights";
    private static final String TARGET_TABLE = "market.stock_dashboard";

    public SparkRankingService(SparkBuilder sparkBuilder) {
        this.sparkBuilder = sparkBuilder;
    }

    public void startRankingJob() {
        SparkSession spark = sparkBuilder.getSparkSession();

        new Thread(() -> {
            while (true) {
                try {
                    // 1. Lấy max_time hiện tại từ bảng dashboard để chỉ quét data mới
                    String maxTimeQuery = "(SELECT COALESCE(MAX(window_time), '1970-01-01 00:00:00'::timestamp) as max_time FROM " + TARGET_TABLE + ") as t";
                    
                    Timestamp maxWindowTime = spark.read().format("jdbc")
                            .option("url", postgresUrl)
                            .option("dbtable", maxTimeQuery)
                            .option("user", postgresUser)
                            .option("password", postgresPassword)
                            .option("driver", "org.postgresql.Driver")
                            .load()
                            .first().getTimestamp(0);

                    // 2. Lấy dữ liệu mới từ stock_insights
                    String sourceQuery = "(SELECT * FROM " + SOURCE_TABLE + " WHERE window_time > '" + maxWindowTime + "') as new_insights";
                    Dataset<Row> df = spark.read().format("jdbc")
                            .option("url", postgresUrl)
                            .option("dbtable", sourceQuery)
                            .option("user", postgresUser)
                            .option("password", postgresPassword)
                            .option("driver", "org.postgresql.Driver")
                            .load();

                    if (df.isEmpty()) {
                        Thread.sleep(3000);
                        continue;
                    }

                    // 3. Tính toán Scoring
                    Dataset<Row> ranked = df
                            .withColumn("momentum_score",
                                    col("obi_score").multiply(0.4)
                                            .plus(col("buy_active_ratio").multiply(0.3))
                                            .plus(col("price_pos").multiply(0.3)))
                            .withColumn("total_score",
                                    col("momentum_score")
                                            .plus(log1p(when(col("money_flow").gt(1000000), col("money_flow")).otherwise(lit(1))))
                                            .plus(log1p(when(col("net_volume").gt(100000), col("net_volume")).otherwise(lit(1)))));

                    // 4. Ranking theo window_time
                    WindowSpec windowSpec = Window.partitionBy("window_time").orderBy(col("total_score").desc());
                    
                    ranked = ranked.withColumn("rank", row_number().over(windowSpec))
                            .withColumn("insight_label",
                                    when(col("total_score").gt(15).and(col("rank").leq(3)), "🔥 HOT")
                                            .when(col("rank").leq(10), "⭐ WATCHLIST")
                                            .when(col("signal").equalTo("STRONG_BUY").and(col("net_volume").gt(200000)), "💰 BUY_SIGNAL")
                                            .when(col("signal").equalTo("NO_ACTIVITY"), "NO_ACTIVITY")
                                            .otherwise("NORMAL"));

                    // 5. Loại bỏ trùng lặp (nếu có) trước khi ghi
                    ranked = ranked.dropDuplicates("window_time", "symbol");

                    // 6. Ghi kết quả vào database
                    ranked.select(
                            col("window_time"), col("symbol"), col("obi_score"), col("buy_active_ratio"),
                            col("net_volume"), col("money_flow"), col("price_pos"), col("signal"),
                            col("liquidity_status"), col("momentum_score"), col("total_score"),
                            col("rank"), col("insight_label")
                    ).write()
                            .format("jdbc")
                            .option("url", postgresUrl)
                            .option("dbtable", TARGET_TABLE)
                            .option("user", postgresUser)
                            .option("password", postgresPassword)
                            .option("driver", "org.postgresql.Driver")
                            .mode("append")
                            .save();

                    System.out.println("✅ [JOB 3] Inserted " + ranked.count() + " rows to dashboard");
                    Thread.sleep(3000);

                } catch (Exception e) {
                    System.err.println("❌ [JOB 3 ERROR]: " + e.getMessage());
                    e.printStackTrace();
                    try { Thread.sleep(10000); } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }).start();
    }
}