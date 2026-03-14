package com.em_loc.demo;

import org.apache.spark.sql.SparkSession;

public class SparkBuilder {
    private static SparkSession spark;
    
    public static SparkSession getSparkSession() {
        if (spark == null) {
            spark = SparkSession.builder()
                    .appName("MySparkApplication")
                    .master("local[*]")
                    .config("spark.sql.shuffle.partitions", "200")
                    .getOrCreate();
        }
        return spark;
    }
}
