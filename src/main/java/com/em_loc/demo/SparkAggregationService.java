package com.em_loc.demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import java.io.Serializable;

@Service
public class SparkAggregationService implements Serializable {

    private final SparkBuilder sparkBuilder;

    @Value("${postgres.url}") private String postgresUrl;
    @Value("${postgres.user}") private String postgresUser;
    @Value("${postgres.password}") private String postgresPassword;
    @Value("${kafka.bootstrap-servers:localhost:9095}") private String bootstrapServers;

    private static final String SOURCE_TOPIC = "CLEAN-STOCK-DATA";
    private static final String INSIGHT_TABLE = "market.stock_insights";

    public SparkAggregationService(SparkBuilder sparkBuilder) {
        this.sparkBuilder = sparkBuilder;
    }

    public StreamingQuery startAggregation() throws Exception {
        SparkSession spark = sparkBuilder.getSparkSession();
        
        // Cấu hình để Spark không kiểm tra schema cũ nếu bạn thay đổi logic
        spark.conf().set("spark.sql.streaming.stateStore.stateSchemaCheck", "false");

        StructType schema = new StructType()
                .add("symbol", DataTypes.StringType)
                .add("close_price", DataTypes.IntegerType)
                .add("high_price", DataTypes.IntegerType)
                .add("low_price", DataTypes.IntegerType)
                .add("avg_price", DataTypes.DoubleType)
                .add("volume", DataTypes.LongType)
                .add("bid_volume_1", DataTypes.LongType)
                .add("bid_volume_2", DataTypes.LongType)
                .add("bid_volume_3", DataTypes.LongType)
                .add("ask_volume_1", DataTypes.LongType)
                .add("ask_volume_2", DataTypes.LongType)
                .add("ask_volume_3", DataTypes.LongType)
                .add("trade_type", DataTypes.StringType)
                .add("created_at", DataTypes.StringType);

        Dataset<Row> cleanStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", SOURCE_TOPIC)
                .option("startingOffsets", "earliest")
                .load()
                .select(from_json(col("value").cast("string"), schema).alias("data"))
                .select("data.*")
                .withColumn("created_at", coalesce(
                        to_timestamp(col("created_at"), "yyyy-MM-dd HH:mm:ss.SSS"),
                        to_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
                        to_timestamp(col("created_at"))  // thêm format fallback
                ));

        // AGGREGATION LOGIC
        Dataset<Row> insights = cleanStream
                .withWatermark("created_at", "10 minutes")   // tăng watermark cho ổn định
                .groupBy(window(col("created_at"), "5 minutes", "1 minute"), col("symbol"))
                .agg(
                        // FIX: Đã bọc hàm last() cho các tính toán bid/ask volume để lấy trạng thái sổ lệnh cuối window
                        coalesce(
                            when(
                                last(col("bid_volume_1").plus(col("bid_volume_2")).plus(col("bid_volume_3"))
                                    .plus(col("ask_volume_1")).plus(col("ask_volume_2")).plus(col("ask_volume_3"))).gt(0),
                                
                                last(col("bid_volume_1").plus(col("bid_volume_2")).plus(col("bid_volume_3")))
                                    .minus(last(col("ask_volume_1").plus(col("ask_volume_2")).plus(col("ask_volume_3"))))
                                    .divide(
                                        last(col("bid_volume_1").plus(col("bid_volume_2")).plus(col("bid_volume_3"))
                                            .plus(col("ask_volume_1")).plus(col("ask_volume_2")).plus(col("ask_volume_3"))).plus(1)
                                    )
                            ).otherwise(lit(0.0))
                        ).alias("obi_score"),
                        
                        // Các cột khác đã được aggregate đúng chuẩn
                        last("volume").minus(first("volume")).alias("net_volume"),
                        avg(when(col("trade_type").equalTo("BUY_ACTIVE"), 1.0).otherwise(0.0)).alias("buy_active_ratio"),
                        avg("avg_price").multiply(last("volume").minus(first("volume"))).alias("money_flow"),
                        last("close_price").alias("window_close"),
                        max("high_price").alias("window_high"),
                        min("low_price").alias("window_low")
                )
                .withColumn("price_pos",   // Clamping giá trị trong khoảng [0, 1]
                        when(col("window_high").minus(col("window_low")).gt(0),
                            greatest(lit(0.0),
                                least(lit(1.0),
                                    col("window_close").minus(col("window_low"))
                                        .divide(col("window_high").minus(col("window_low")))
                                )
                            )
                        ).otherwise(0.5))
                .withColumn("is_volume_spike", col("net_volume").gt(100000))
                .withColumn("liquidity_status", when(col("obi_score").gt(0.2), "HIGH")
                        .when(col("obi_score").lt(-0.2), "LOW").otherwise("BALANCED"))
                
                // Signal nghiêm ngặt hơn
                .withColumn("signal",
                        when(col("net_volume").leq(50000), "NO_ACTIVITY")
                        .when(col("obi_score").gt(0.3).and(col("net_volume").gt(100000)).and(col("price_pos").gt(0.6)), "STRONG_BUY")
                        .when(col("obi_score").gt(0.2).and(col("price_pos").gt(0.7)), "BREAKOUT_CANDIDATE")
                        .when(col("obi_score").lt(-0.3).and(col("net_volume").gt(50000)), "DISTRIBUTION")
                        .otherwise("NEUTRAL"))
                
                .withColumn("window_time", col("window.end"))
                .drop("window", "window_close", "window_high", "window_low");

        // WRITE STREAM
        return insights.writeStream()
                .outputMode("update")
                .trigger(Trigger.ProcessingTime("5 seconds"))
                .foreachBatch((batchDF, batchId) -> {
                    if (batchDF.isEmpty()) return;

                    // Loại bỏ trùng lặp trong cùng 1 window của 1 mã trước khi lưu
                    Dataset<Row> finalBatch = batchDF.dropDuplicates("symbol", "window_time");

                    finalBatch.write()
                            .format("jdbc")
                            .option("url", postgresUrl)
                            .option("dbtable", INSIGHT_TABLE)
                            .option("user", postgresUser)
                            .option("password", postgresPassword)
                            .option("driver", "org.postgresql.Driver")
                            .mode("append")
                            .save();

                    System.out.println("💾 [JOB 2] Batch " + batchId + " → " + finalBatch.count() + " rows");
                })
                .option("checkpointLocation", "D:/spark-checkpoint-job2")
                .start();
    }
}