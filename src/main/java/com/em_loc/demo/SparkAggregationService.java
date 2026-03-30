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

        // 🛠️ Tắt kiểm tra schema để tránh lỗi khi thay đổi logic (giúp phát triển nhanh)
        spark.conf().set("spark.sql.streaming.stateStore.stateSchemaCheck", "false");

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

        Dataset<Row> cleanStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", SOURCE_TOPIC)
                .option("startingOffsets", "earliest")
                .load()
                .select(from_json(col("value").cast("string"), schema).alias("data"))
                .select("data.*");

        Dataset<Row> insights = cleanStream
                .withWatermark("created_at", "10 minutes")
                .groupBy(window(col("created_at"), "5 minutes", "1 minute"), col("symbol"))
                .agg(
                    // 1. Chỉ số OBI (Order Book Imbalance)
                    avg((col("bid_volume_1").plus(col("bid_volume_2")).plus(col("bid_volume_3"))
                        .minus(col("ask_volume_1").plus(col("ask_volume_2")).plus(col("ask_volume_3"))))
                        .divide(col("bid_volume_1").plus(col("bid_volume_2")).plus(col("bid_volume_3"))
                        .plus(col("ask_volume_1")).plus(col("ask_volume_2")).plus(col("ask_volume_3")).plus(1)))
                        .alias("obi_score"),

                    // 2. Net Volume (Khối lượng khớp lệnh thực tế trong Window)
                    (last("volume").minus(first("volume"))).alias("net_volume"),

                    // 3. Average Volume (Dùng để xác định thanh khoản)
                    avg("volume").alias("avg_volume"),

                    // 4. Vị thế giá (Price Position so với High/Low)
                    avg(col("close_price").minus(col("low_price"))
                        .divide(col("high_price").minus(col("low_price")).plus(1)))
                        .alias("price_pos"),

                    avg("close_price").alias("avg_close"),
                    last("ceiling_price").alias("ceil"),

                    // 5. Thống kê ticks mua chủ động
                    sum(when(col("trade_type").equalTo("BUY_ACTIVE"), 1).otherwise(0)).alias("buy_active_ticks"),
                    count("trade_type").alias("total_ticks")
                )
                .withColumn("money_flow", col("avg_close").multiply(col("net_volume")))
                .withColumn("buy_active_ratio",
                    round(col("buy_active_ticks").cast("double")
                        .divide(when(col("total_ticks").equalTo(0), 1).otherwise(col("total_ticks"))), 2))
                
                // ✨ Bổ sung logic Liquidity Status (Dành cho Job 3)
                .withColumn("liquidity_status",
                    when(col("avg_volume").gt(200000), "HIGH")
                    .when(col("avg_volume").gt(50000), "MEDIUM")
                    .otherwise("LOW"))

                // ✨ Logic phát tín hiệu Signal
                .withColumn("signal",
                    when(col("buy_active_ratio").gt(0.6).and(col("obi_score").gt(0.3)).and(col("net_volume").gt(50000)), "STRONG_BUY")
                    .when(col("ceil").minus(col("avg_close")).lt(100).and(col("obi_score").gt(0.2)), "BREAKOUT_CANDIDATE")
                    .when(col("buy_active_ratio").lt(0.4).and(col("obi_score").lt(-0.3)), "DISTRIBUTION")
                    .otherwise("NEUTRAL"))
                .withColumn("window_time", col("window.end"));

        StreamingQuery query = insights.writeStream()
                .outputMode("update")
                .foreachBatch((batchDF, batchId) -> {
                    // Loại bỏ trùng lặp trước khi ghi vào DB
                    Dataset<Row> finalBatch = batchDF.dropDuplicates("symbol", "window_time");

                    finalBatch.select(
                            col("window_time"),
                            col("symbol"),
                            col("obi_score"),
                            col("buy_active_ratio"),
                            col("net_volume"),
                            col("money_flow"),
                            col("price_pos"),
                            col("signal"),
                            col("liquidity_status") // ✅ Đã có cột này để Job 3 không báo lỗi
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

                    System.out.println("💾 [JOB 2] Batch " + batchId + " → INSERTED insights (including liquidity) into " + INSIGHT_TABLE);
                })
                .option("checkpointLocation", "D:/spark-checkpoint-job2")
                .start();

        return query;
    }
}