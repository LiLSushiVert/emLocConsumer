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
                    // Lấy thời gian mới nhất trong dashboard
                    String maxTimeQuery = "(SELECT COALESCE(MAX(window_time), '1970-01-01 00:00:00'::timestamp) as max_time FROM " + TARGET_TABLE + ") as t";
                    Dataset<Row> maxTimeDf = spark.read().format("jdbc")
                            .option("url", postgresUrl).option("dbtable", maxTimeQuery)
                            .option("user", postgresUser).option("password", postgresPassword)
                            .option("driver", "org.postgresql.Driver").load();
                    Timestamp maxWindowTime = maxTimeDf.first().getTimestamp(0);

                    // Lấy data mới từ insights
                    String sourceQuery = "(SELECT * FROM " + SOURCE_TABLE + " WHERE window_time > '" + maxWindowTime + "') as new_insights";
                    Dataset<Row> df = spark.read().format("jdbc")
                            .option("url", postgresUrl).option("dbtable", sourceQuery)
                            .option("user", postgresUser).option("password", postgresPassword)
                            .option("driver", "org.postgresql.Driver").load();

                    if (!df.isEmpty()) {
                        System.out.println("🏆 [JOB 3] Found " + df.count() + " new insights to score");

                        // Scoring theo schema JOB 2 mới (không có buy_active_ratio & price_pos)
                        Dataset<Row> ranked = df.withColumn("momentum_score", col("obi_score").multiply(0.6))  // tạm dùng obi_score làm chính
                                .withColumn("total_score", col("momentum_score")
                                        .plus(log1p(abs(col("net_volume"))).multiply(0.4)));

                        WindowSpec windowSpec = Window.partitionBy("window_time").orderBy(col("total_score").desc());
                        ranked = ranked.withColumn("rank", row_number().over(windowSpec))
                                .withColumn("insight_label",
                                        when(col("rank").leq(3).and(col("total_score").gt(1.0)), "🔥 HOT")
                                        .when(col("rank").leq(10), "⭐ WATCHLIST")
                                        .when(col("signal").equalTo("STRONG_BUY"), "💰 BUY_SIGNAL")
                                        .otherwise("NORMAL"));

                        ranked.select("window_time", "symbol", "obi_score", "net_volume",
                                "signal", "liquidity_status", "rank", "insight_label", "total_score", "momentum_score")
                                .write().format("jdbc")
                                .option("url", postgresUrl).option("dbtable", TARGET_TABLE)
                                .option("user", postgresUser).option("password", postgresPassword)
                                .option("driver", "org.postgresql.Driver")
                                .mode("append").save();

                        System.out.println("✅ [JOB 3 SUCCESS] Inserted " + ranked.count() + " rows into stock_dashboard");
                    } else {
                        System.out.println("⏳ [JOB 3] No new data yet");
                    }

                    Thread.sleep(5000); // poll nhanh hơn để test
                } catch (Exception e) {
                    e.printStackTrace();
                    try { Thread.sleep(10000); } catch (InterruptedException ie) {}
                }
            }
        }).start();
    }
}