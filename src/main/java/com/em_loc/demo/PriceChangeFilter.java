package com.em_loc.demo;

import org.apache.spark.sql.Row;
import redis.clients.jedis.Jedis;

public class PriceChangeFilter {

    private static final double THRESHOLD = 0.001;

    public boolean shouldEmit(Jedis jedis, Row row, boolean isDbEmpty) {
        String symbol = row.getAs("symbol");
        Integer currentPrice = row.getAs("close_price");
        Long currentVolume = row.getAs("volume");

        if (symbol == null || currentPrice == null || currentVolume == null) {
            return false;
        }

        String key = "stock_state:" + symbol;
        
        String oldData = jedis.get(key);
        if (isDbEmpty || oldData == null) {
            saveToRedis(jedis, key, currentPrice, currentVolume);
            return true;
        }

        String[] parts = oldData.split(":");
        int oldPrice = Integer.parseInt(parts[0]);
        long oldVolume = Long.parseLong(parts[1]);

        double priceChangePct = (double) Math.abs(currentPrice - oldPrice) / (oldPrice == 0 ? 1 : oldPrice);
        boolean isPriceSignificantlyChanged = (priceChangePct > THRESHOLD);
        boolean isVolumeChanged = (currentVolume != oldVolume);

        if (isPriceSignificantlyChanged || isVolumeChanged) {
            saveToRedis(jedis, key, currentPrice, currentVolume);
            return true;
        }
        return false;
    }

    private void saveToRedis(Jedis jedis, String key, Integer price, Long volume) {
        jedis.set(key, price + ":" + volume);
    }
}