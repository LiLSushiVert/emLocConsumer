package com.em_loc.demo;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.apache.spark.sql.SparkSession;

@Component
public class SparkBuilder {
     private SparkSession spark;

     @Value("${spark.app.name}")
    private String appName;

    @Value("${spark.master}")
    private String master;

    @Value("${spark.sql.shuffle.partitions}")
    private String shufflePartitions;

    public SparkSession getSparkSession() {
        if (spark == null) {
            spark = SparkSession.builder()
                    .appName(appName)
                    .master(master)
                    .config("spark.sql.shuffle.partitions", shufflePartitions)
                    .getOrCreate();
        }
        return spark;
    }
}
