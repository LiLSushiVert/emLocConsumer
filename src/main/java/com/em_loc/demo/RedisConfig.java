package com.em_loc.demo;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

@Configuration
public class RedisConfig {
    @Bean
    public JedisPool jedisPool() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(100);
        poolConfig.setMaxIdle(30);
        poolConfig.setMinIdle(10);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setMaxWaitMillis(30000);

        int timeoutMs = 30000;
        String host = "localhost";
        int port = 6379;

        System.out.println("🔗 [RedisConfig] JedisPool created - timeout=" + timeoutMs + "ms");

        return new JedisPool(poolConfig, host, port, timeoutMs);
    }
}
