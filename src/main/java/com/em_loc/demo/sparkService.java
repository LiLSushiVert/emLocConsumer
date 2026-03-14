package com.em_loc.demo;

import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Service;

@Service
public class SparkService {

    private final SparkBuilder sparkBuilder;

    public SparkService(SparkBuilder sparkBuilder) {
        this.sparkBuilder = sparkBuilder;
    }

    public SparkSession getSpark() {
        return sparkBuilder.getSparkSession();
    }
}