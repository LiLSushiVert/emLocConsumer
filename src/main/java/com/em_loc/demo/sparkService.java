package com.em_loc.demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class SparkService {

private final SparkBuilder sparkBuilder;

@Value("${postgres.url}")
private String postgresUrl;

@Value("${postgres.user}")
private String postgresUser;

@Value("${postgres.password}")
private String postgresPassword;

@Value("${postgres.table}")
private String postgresTable;

public SparkService(SparkBuilder sparkBuilder) {
    this.sparkBuilder = sparkBuilder;
}

public SparkSession getSparkSession() {
    return sparkBuilder.getSparkSession();
}

public void readKafka() {

    SparkSession spark = sparkBuilder.getSparkSession();

    Dataset<Row> df = spark
            .read()
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9095")
            .option("subscribe", "LOC-VIETCAP-STOCK-DATA-TOPIC")
            .option("startingOffsets", "earliest")
            .load();

    Dataset<Row> messages = df.selectExpr("CAST(value AS STRING) as json");

    StructType schema = new StructType()
            .add("co", DataTypes.StringType)
            .add("s", DataTypes.StringType)
            .add("c", DataTypes.IntegerType)
            .add("h", DataTypes.IntegerType)
            .add("l", DataTypes.IntegerType)
            .add("op", DataTypes.IntegerType)
            .add("ref", DataTypes.IntegerType)
            .add("avgp", DataTypes.DoubleType)
            .add("vo", DataTypes.LongType)
            .add("va", DataTypes.DoubleType)
            .add("bo", DataTypes.StringType);

    Dataset<Row> parsed = messages
            .select(from_json(col("json"), schema).alias("data"))
            .select("data.*");

    Dataset<Row> transformed = parsed
            .withColumnRenamed("co", "code")
            .withColumnRenamed("s", "symbol")
            .withColumnRenamed("c", "close_price")
            .withColumnRenamed("h", "high_price")
            .withColumnRenamed("l", "low_price")
            .withColumnRenamed("op", "open_price")
            .withColumnRenamed("ref", "ref_price")
            .withColumnRenamed("avgp", "avg_price")
            .withColumnRenamed("vo", "volume")
            .withColumnRenamed("va", "value")
            .withColumnRenamed("bo", "exchange");

    transformed.show();

    transformed.write()
            .format("jdbc")
            .option("url", postgresUrl)
            .option("dbtable", postgresTable)
            .option("user", postgresUser)
            .option("password", postgresPassword)
            .mode("append")
            .save();
    }
}
