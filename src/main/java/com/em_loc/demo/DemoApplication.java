package com.em_loc.demo;

import org.apache.spark.sql.SparkSession;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}

	@Bean
	CommandLineRunner run(SparkService sparkService) {
		return args -> {

			SparkSession spark = sparkService.getSparkSession();

			System.out.println("Spark client started: " + spark.sparkContext().appName());

		};
	}
}