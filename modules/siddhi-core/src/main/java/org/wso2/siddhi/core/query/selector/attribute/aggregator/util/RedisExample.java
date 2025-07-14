//package org.wso2.siddhi.core.query.selector.attribute.aggregator.util;
//
//
//import redis.clients.jedis.Jedis;
//
//public class RedisExample {
//    public static void main(String[] args) {
//        // Replace with your Redis host and port if needed
//        String redisHost = "localhost";
//        int redisPort = 6379;
//
//        // Connect to Redis
//        try (Jedis jedis = new Jedis(redisHost, redisPort)) {
//            System.out.println("Connected to Redis");
//
//            // Set a key
//            jedis.set("mykey", "Hello Redis");
//
//            // Get the value
//            String value = jedis.get("mykey");
//            System.out.println("Stored value in Redis: " + value);
//        } catch (Exception e) {
//            System.err.println("Redis connection failed: " + e.getMessage());
//        }
//    }
//}
//
