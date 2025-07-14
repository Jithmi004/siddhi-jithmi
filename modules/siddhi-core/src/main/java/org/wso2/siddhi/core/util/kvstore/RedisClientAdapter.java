package org.wso2.siddhi.core.util.kvstore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.exceptions.JedisException;

/**
 * Redis adapter using JedisCluster for distributed Redis setups (e.g., AWS ElastiCache Redis Cluster).
 */
public class RedisClientAdapter implements KeyValueStoreClient {

    private static final Logger log = LoggerFactory.getLogger(RedisClientAdapter.class);

    @Override
    public void connect() {
        log.info("Initializing Redis connection (JedisCluster)");
        try {
            RedisConnectionManager.getJedisCluster();
        } catch (Exception e) {
            log.error("Error while initializing Redis cluster connection", e);
        }
    }

    @Override
    public void disconnect() {
        log.info("Shutting down Redis cluster connection");
        RedisConnectionManager.shutdownCluster();
    }

    @Override
    public boolean isConnected() {
        try {
            String pong = RedisConnectionManager.getJedisCluster().ping();
            return "PONG".equalsIgnoreCase(pong);
        } catch (JedisException e) {
            log.warn("Redis ping failed", e);
            return false;
        }
    }

    @Override
    public String get(String key) {
        if (key == null) return null;
        try {
            return RedisConnectionManager.getJedisCluster().get(key);
        } catch (JedisException e) {
            log.error("Error during GET for key '{}'", key, e);
            throw new KeyValueStoreException("GET failed for key: " + key, e);
        }
    }

    @Override
    public void set(String key, String value) {
        if (key == null) throw new KeyValueStoreException("Key cannot be null for SET");
        try {
            RedisConnectionManager.getJedisCluster().set(key, value);
        } catch (JedisException e) {
            log.error("Error during SET for key '{}'", key, e);
            throw new KeyValueStoreException("SET failed for key: " + key, e);
        }
    }

    @Override
    public long increment(String key) {
        if (key == null) throw new KeyValueStoreException("Key cannot be null for INCR");
        try {
            return RedisConnectionManager.getJedisCluster().incr(key);
        } catch (JedisException e) {
            log.error("Error during INCR for key '{}'", key, e);
            throw new KeyValueStoreException("INCR failed for key: " + key, e);
        }
    }

    @Override
    public long decrement(String key) {
        if (key == null) throw new KeyValueStoreException("Key cannot be null for DECR");
        try {
            return RedisConnectionManager.getJedisCluster().decr(key);
        } catch (JedisException e) {
            log.error("Error during DECR for key '{}'", key, e);
            throw new KeyValueStoreException("DECR failed for key: " + key, e);
        }
    }

    @Override
    public void delete(String key) {
        if (key == null) throw new KeyValueStoreException("Key cannot be null for DEL");
        try {
            RedisConnectionManager.getJedisCluster().del(key);
        } catch (JedisException e) {
            log.error("Error during DEL for key '{}'", key, e);
            throw new KeyValueStoreException("DEL failed for key: " + key, e);
        }
    }
}
