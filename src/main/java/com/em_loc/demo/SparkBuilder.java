package com.em_loc.demo;

import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkBuilder {

    @Bean(destroyMethod = "stop")
    public SparkSession getSparkSession() {
        return SparkSession.builder()
                .appName("Stock-Insights-Spark-Streaming")
                .master("local[*]")

                // ==================== FIX LỖI RENAME + WINDOWS ====================
                .config("spark.local.dir", "D:/spark-tmp")
                .config("spark.sql.shuffle.partitions", "20")
                .config("spark.file.transferTo", "false")
                .config("spark.shuffle.io.maxRetries", "10")
                .config("spark.shuffle.io.retryWait", "5s")
                .config("spark.shuffle.sort.bypassMergeThreshold", "200")

                // ==================== MEMORY ====================
                .config("spark.memory.fraction", "0.7")
                .config("spark.memory.storageFraction", "0.3")
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "4g")

                // ==================== STREAMING ====================
                .config("spark.streaming.backpressure.enabled", "true")
                .config("spark.streaming.kafka.maxRatePerPartition", "1000")

                // ==================== TẮT SPARK UI (FIX LỖI HIỆN TẠI) ====================
                .config("spark.ui.enabled", "false")

                // Tùy chọn khác (khuyến nghị)
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.ui.showConsoleProgress", "false")

                .getOrCreate();
    }
}