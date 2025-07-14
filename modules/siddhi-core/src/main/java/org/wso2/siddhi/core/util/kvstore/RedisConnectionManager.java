package org.wso2.siddhi.core.util.kvstore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Set;

public class RedisConnectionManager {

    private static final Logger log = LoggerFactory.getLogger(RedisConnectionManager.class);
    private static volatile JedisCluster jedisCluster;

    private static final String DEFAULT_REDIS_HOST = "localhost";
    private static final int DEFAULT_REDIS_PORT = 6379;

    public static JedisCluster getJedisCluster() {
        if (jedisCluster == null) {
            synchronized (RedisConnectionManager.class) {
                if (jedisCluster == null) {
                    String redisHost = System.getProperty("redis.host", DEFAULT_REDIS_HOST);
                    int redisPort;
                    try {
                        redisPort = Integer.parseInt(System.getProperty("redis.port", String.valueOf(DEFAULT_REDIS_PORT)));
                    } catch (NumberFormatException e) {
                        log.warn("Invalid Redis port in system property 'redis.port'. Using default: {}", DEFAULT_REDIS_PORT, e);
                        redisPort = DEFAULT_REDIS_PORT;
                    }

                    Set<HostAndPort> nodes = new HashSet<>();
                    nodes.add(new HostAndPort(redisHost, redisPort));

                    try {
                        jedisCluster = new JedisCluster(nodes);
                        log.info("Initialized JedisCluster for Redis at {}:{}", redisHost, redisPort);
                    } catch (Exception e) {
                        log.error("Failed to initialize JedisCluster", e);
                    }
                }
            }
        }
        return jedisCluster;
    }

    public static void shutdownCluster() {
        JedisCluster cluster = jedisCluster;
        jedisCluster = null;
        if (cluster != null) {
            try {
                cluster.close();
                log.info("JedisCluster closed.");
            } catch (Exception e) {
                log.error("Error closing JedisCluster", e);
            }
        } else {
            log.info("JedisCluster not initialized or already closed.");
        }
    }
}
