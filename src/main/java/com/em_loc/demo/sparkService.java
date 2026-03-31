package com.em_loc.demo;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;

@Service
public class SparkService {
    private final SparkBuilder sparkBuilder;

    @Value("${postgres.url}") private String postgresUrl;
    @Value("${postgres.user}") private String postgresUser;
    @Value("${postgres.password}") private String postgresPassword;
    @Value("${kafka.bootstrap-servers:localhost:9095}") private String bootstrapServers;

    private static final String TARGET_TABLE = "market.stock_ticks2";
    private static final String SOURCE_TOPIC = "LOC-VIETCAP-STOCK-DATA-TOPIC";
    private static final String CLEAN_DATA_TOPIC = "CLEAN-STOCK-DATA";

    public SparkService(SparkBuilder sparkBuilder) {
        this.sparkBuilder = sparkBuilder;
    }

    public StreamingQuery readKafka() throws Exception {
        SparkSession spark = sparkBuilder.getSparkSession();

        StructType schema = new StructType()
            .add("co", DataTypes.StringType).add("s", DataTypes.StringType)
            .add("orgn", DataTypes.StringType).add("enorgn", DataTypes.StringType)
            .add("st", DataTypes.StringType).add("bo", DataTypes.StringType)
            .add("ref", DataTypes.IntegerType).add("op", DataTypes.IntegerType)
            .add("c", DataTypes.IntegerType).add("h", DataTypes.IntegerType)
            .add("l", DataTypes.IntegerType).add("cei", DataTypes.IntegerType)
            .add("flo", DataTypes.IntegerType).add("avgp", DataTypes.DoubleType)
            .add("vo", DataTypes.LongType).add("va", DataTypes.DoubleType)
            .add("frbv", DataTypes.LongType).add("frsv", DataTypes.LongType)
            .add("frcrr", DataTypes.LongType).add("bp1", DataTypes.StringType)
            .add("bv1", DataTypes.LongType).add("bp2", DataTypes.IntegerType)
            .add("bv2", DataTypes.LongType).add("bp3", DataTypes.IntegerType)
            .add("bv3", DataTypes.LongType).add("ap1", DataTypes.IntegerType)
            .add("av1", DataTypes.LongType).add("ap2", DataTypes.IntegerType)
            .add("av2", DataTypes.LongType).add("ap3", DataTypes.IntegerType)
            .add("av3", DataTypes.LongType).add("ptv", DataTypes.LongType)
            .add("pta", DataTypes.LongType);

        Dataset<Row> kafkaStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", SOURCE_TOPIC)
                .option("startingOffsets", "earliest")
                .load();

        Dataset<Row> transformed = kafkaStream
                .select(from_json(col("value").cast("string"), schema).alias("data"), col("timestamp"), col("offset"))
                .select(
                        col("data.co").alias("code"), col("data.s").alias("symbol"),
                        col("data.orgn").alias("company_name"),
                        col("data.enorgn").alias("company_name_en"),
                        col("data.bo").alias("exchange"),
                        col("data.st").alias("stock_type"),
                        col("data.ref").alias("ref_price"),
                        col("data.op").alias("open_price"),
                        col("data.c").alias("close_price"),
                        col("data.h").alias("high_price"),
                        col("data.l").alias("low_price"),
                        col("data.flo").alias("floor_price"),
                        col("data.cei").alias("ceiling_price"),
                        col("data.avgp").alias("avg_price"),
                        col("data.vo").alias("volume"),
                        col("data.va").alias("value"),
                        col("data.frbv").alias("foreign_buy_volume"),
                        col("data.frsv").alias("foreign_sell_volume"),
                        col("data.frcrr").alias("foreign_room"),
                        col("data.bp1").cast("int").alias("bid_price_1"),
                        col("data.bv1").alias("bid_volume_1"),
                        col("data.bp2").alias("bid_price_2"),
                        col("data.bv2").alias("bid_volume_2"),
                        col("data.bp3").alias("bid_price_3"),
                        col("data.bv3").alias("bid_volume_3"),
                        col("data.ap1").alias("ask_price_1"),
                        col("data.av1").alias("ask_volume_1"),
                        col("data.ap2").alias("ask_price_2"),
                        col("data.av2").alias("ask_volume_2"),
                        col("data.ap3").alias("ask_price_3"),
                        col("data.av3").alias("ask_volume_3"),
                        col("data.ptv").alias("put_through_volume"),
                        col("data.pta").alias("put_through_value"),
                        col("timestamp").alias("created_at"),
                        col("offset").alias("offsetkafka")
                );

        return transformed.writeStream()
                .foreachBatch((batchDF, batchId) -> {
                    if (batchDF.isEmpty()) return;

                    Dataset<Row> latestPerSymbol = batchDF.orderBy(col("created_at").desc()).dropDuplicates("symbol");
                    List<Row> rows = latestPerSymbol.collectAsList();
                    List<Row> filteredRows = new ArrayList<>();
                    PriceChangeFilter filter = new PriceChangeFilter();

                    try (Jedis jedis = new Jedis("localhost", 6379)) {
                        for (Row r : rows) {
                            if (filter.shouldEmit(jedis, r, false)) {
                                filteredRows.add(r);
                            }
                        }
                    }

                    if (!filteredRows.isEmpty()) {
                        Dataset<Row> filteredDF = batchDF.sparkSession().createDataFrame(filteredRows, batchDF.schema());

                        Dataset<Row> enrichedDF = filteredDF
                            .withColumn("change_percent", round(when(col("ref_price").notEqual(0),
                                 col("close_price").minus(col("ref_price")).divide(col("ref_price")).multiply(100)).otherwise(0), 2))
                            .withColumn("price_direction", when(col("close_price").gt(col("ref_price")), "UP")
                                 .when(col("close_price").lt(col("ref_price")), "DOWN").otherwise("FLAT"))
                            .withColumn("spread", col("ask_price_1").minus(col("bid_price_1")))
                            .withColumn("turnover_million", round(col("value").multiply(1000).divide(1_000_000), 2))
                            .withColumn("trade_type",
                                 when(col("close_price").geq(col("ask_price_1")), "BUY_ACTIVE")
                                .when(col("close_price").leq(col("bid_price_1")), "SELL_ACTIVE")
                                .otherwise("NEUTRAL"))

                            // ==================== 4 CỘT MỚI – ĐÃ FIX NULL HOÀN TOÀN ====================
                            .withColumn("dist_to_vwap_pct",
                                round(when(col("avg_price").isNotNull().and(col("avg_price").gt(0)),
                                    col("close_price").cast("double").minus(col("avg_price"))
                                        .divide(col("avg_price")).multiply(100))
                                    .otherwise(0), 2))

                            .withColumn("dist_to_floor_pct",
                                round(when(col("floor_price").isNotNull().and(col("floor_price").gt(0)),
                                    col("close_price").cast("double").minus(col("floor_price"))
                                        .divide(col("floor_price")).multiply(100))
                                    .otherwise(0), 2))

                            .withColumn("dist_to_ceiling_pct",
                                round(when(col("ceiling_price").isNotNull().and(col("ceiling_price").gt(0)),
                                    col("ceiling_price").cast("double").minus(col("close_price"))
                                        .divide(col("ceiling_price")).multiply(100))
                                    .otherwise(0), 2))

                            .withColumn("intraday_vol_pct",
                                round(coalesce(col("volume").cast("double").divide(1000000.0), lit(0.0)), 2));

                        // Save to Postgres
                        enrichedDF.write().format("jdbc")
                                .option("url", postgresUrl).option("dbtable", TARGET_TABLE)
                                .option("user", postgresUser).option("password", postgresPassword)
                                .mode("append").save();

                        // Push to Kafka CLEAN
                        enrichedDF.select(to_json(struct(col("*"))).alias("value"))
                                .write().format("kafka")
                                .option("kafka.bootstrap.servers", bootstrapServers)
                                .option("topic", CLEAN_DATA_TOPIC).save();
                    }
                })
                .option("checkpointLocation", "D:/spark-checkpoint-job1")
                .start();
    }
}