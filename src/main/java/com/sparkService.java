package com;

import org.springframework.stereotype.Service;

import com.em_loc.demo.SparkBuilder;

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
