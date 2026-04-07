package com.em_loc.demo;

import org.apache.spark.sql.streaming.StreamingQuery;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class DemoApplication {
    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

    @Bean
    CommandLineRunner runJobs(SparkService sparkService,
                              SparkAggregationService aggregationService,
                              SparkRankingService rankingService,
                              SparkBuilder sparkBuilder) {
        return args -> {
            try {
                System.out.println("🚀 [JOB 1] Starting Kafka Ingestion & Filter...");
                StreamingQuery query1 = sparkService.readKafka();

                System.out.println("📊 [JOB 2] Starting Insight Aggregation (Windowing)...");
                StreamingQuery query2 = aggregationService.startAggregation();

                System.out.println("🏆 [JOB 3] Starting Scoring & Ranking...");
                StreamingQuery query3 = rankingService.startRankingJob();

                System.out.println("✅ Cả 3 Job Streaming & Batch đã được khởi chạy song song.");
                
                // Block thread chính để duy trì Spark Session
                sparkBuilder.getSparkSession().streams().awaitAnyTermination();
            } catch (Exception e) {
                System.err.println("❌ Lỗi khởi chạy job: " + e.getMessage());
                e.printStackTrace();
            }
        };
    }
}