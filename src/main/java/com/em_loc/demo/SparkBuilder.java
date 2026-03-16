package com.em_loc.demo;

import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Component;

@Component
public class SparkBuilder {

    private SparkSession sparkSession;

    public SparkSession getSparkSession() {

        if (sparkSession == null) {

            sparkSession = SparkSession.builder()
                    .appName("MySparkApplication")
                    .master("local[*]")                
                    .config("spark.jars.packages",
                            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")                
                    .config("spark.executor.memory", "1g")
                    .config("spark.driver.memory", "1g")
                    .config("spark.ui.enabled", "false")
                    .getOrCreate();
        }

        return sparkSession;
    }
}