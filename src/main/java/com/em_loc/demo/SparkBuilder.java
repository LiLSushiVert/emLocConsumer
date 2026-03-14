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
                    .config("spark.ui.enabled", "false")
                    .getOrCreate();
        }

        return sparkSession;
    }
}