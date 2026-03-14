package com.em_loc.demo;

import org.springframework.stereotype.Service;


@Service
public class sparkService {
     private final SparkBuilder sparkBuilder;

    public SparkService(SparkBuilder sparkBuilder) {
        this.sparkBuilder = sparkBuilder;
    }

    public void runJob() {
        SparkSession spark = sparkBuilder.getSparkSession();
        spark.range(10).show();
    }
}
