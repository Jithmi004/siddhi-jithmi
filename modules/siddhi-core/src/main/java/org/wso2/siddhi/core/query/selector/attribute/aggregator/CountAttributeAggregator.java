/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.siddhi.core.query.selector.attribute.aggregator;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.selector.QuerySelector;
import org.wso2.siddhi.core.util.kvstore.KeyValueStoreClient; // Changed
import org.wso2.siddhi.core.util.kvstore.KeyValueStoreManager; // Changed
import org.wso2.siddhi.core.util.kvstore.KeyValueStoreException; // Added
import org.wso2.siddhi.query.api.definition.Attribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CountAttributeAggregator extends AttributeAggregator {

    private static final Logger log = LoggerFactory.getLogger(CountAttributeAggregator.class);
    private static Attribute.Type type = Attribute.Type.LONG;
    private long value = 0L; // Local fallback counter
    private KeyValueStoreClient kvStoreClient; // Added
    private String kvStoreType; // Added
    private String key;

    private static final CopyOnWriteArrayList<Long> operationDurations = new CopyOnWriteArrayList<>();
    private static final ScheduledExecutorService avgLogger = Executors.newSingleThreadScheduledExecutor();

    static {
        avgLogger.scheduleAtFixedRate(() -> {
            if (!operationDurations.isEmpty()) {
                long sum = 0;
                for (Long duration : operationDurations) {
                    sum += duration;
                }
                double avg = sum / (double) operationDurations.size();
                log.info("Average Redis increment/decrement operation duration in the last minute: {} ms ({} samples)", avg, operationDurations.size());
                operationDurations.clear();
            }
        }, 1, 1, TimeUnit.MINUTES);
    }
    /**
     * The initialization method for FunctionExecutor
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param executionPlanContext         Execution plan runtime context
     */
    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        this.key = QuerySelector.getThreadLocalGroupByKey();

        this.kvStoreType = System.getProperty(
                KeyValueStoreManager.KEYVALUE_STORE_TYPE_PROPERTY,
                KeyValueStoreManager.DEFAULT_KV_STORE_TYPE
        ).toLowerCase();

        try {
            this.kvStoreClient = KeyValueStoreManager.getClient();
            if (this.kvStoreClient != null && this.kvStoreClient.isConnected()) {
                log.info("KeyValueStoreClient of type '{}' initialized and connected for aggregator with key '{}'.",
                        kvStoreType, key);
            } else {
                log.warn("KeyValueStoreClient obtained for type '{}', but isConnected() is false for key '{}'. Aggregator will use fallback.",
                        kvStoreType, key);
                this.kvStoreClient = null; // Ensure fallback
            }
        } catch (KeyValueStoreException e) {
            log.error("Failed to initialize KeyValueStoreClient for aggregator with key '{}'. Reason: {}. Operating in fallback mode.",
                    key, e.getMessage(), e);
            this.kvStoreClient = null; // Ensure fallback
        } catch (Exception e) { // Catch any other unexpected exceptions during client init
            log.error("Unexpected error initializing KeyValueStoreClient for aggregator with key '{}'. Operating in fallback mode.",
                    key, e);
            this.kvStoreClient = null; // Ensure fallback
        }
    }

    public Attribute.Type getReturnType() {
        return type;
    }

    @Override
    public Object processAdd(Object data) {
        long start = System.currentTimeMillis();
        try {
            if (kvStoreClient != null && kvStoreClient.isConnected()) {
                try {
                    long newValue = kvStoreClient.increment(key);
                    value = newValue; // Sync local fallback
                    return newValue;
                } catch (KeyValueStoreException e) {
                    log.error("Error incrementing count in Key-Value store for key '{}'. Falling back to local counter. Error: {}",
                            key, e.getMessage());
                }
            } else {
                if (kvStoreClient == null) {
                    log.trace("processAdd: KeyValueStoreClient is null for key '{}'. Using local counter.", key);
                } else { // Implies !kvStoreClient.isConnected()
                    log.warn("processAdd: KeyValueStoreClient not connected for key '{}'. Using local counter.", key);
                }
            }
            // Fallback to local counter
            value++;
            return value;
        }
        finally {
            long duration = System.currentTimeMillis() - start;
            operationDurations.add(duration);
        }
//        log.info("Incrementing the counter for the key '{}'", key);

    }

    @Override
    public Object processAdd(Object[] data) {
        return processAdd((Object) data);
    }

    @Override
    public Object processRemove(Object data) {
//        log.info("Decrementing the counter for the key '{}'", key);
        long start = System.currentTimeMillis();
        try {
            if (kvStoreClient != null && kvStoreClient.isConnected()) {
                try {
                    long newValue = kvStoreClient.decrement(key);
                    value = newValue; // Sync local fallback
                    return newValue;
                } catch (KeyValueStoreException e) {
                    log.error("Error decrementing count in Key-Value store for key '{}'. Falling back to local counter. Error: {}",
                            key, e.getMessage());
                }
            } else {
                if (kvStoreClient == null) {
                    log.trace("processRemove: KeyValueStoreClient is null for key '{}'. Using local counter.", key);
                } else {
                    log.warn("processRemove: KeyValueStoreClient not connected for key '{}'. Using local counter.", key);
                }
            }
            // Fallback to local counter
            value--;
            return value;
        }
        finally {
            long duration = System.currentTimeMillis() - start;
            operationDurations.add(duration);
        }

    }

    @Override
    public Object processRemove(Object[] data) {
        return processRemove((Object) data);
    }

    @Override
    public Object reset() {
        log.info("Resetting the counter for the key '{}'", key);
        if (kvStoreClient != null && kvStoreClient.isConnected()) {
            try {
                kvStoreClient.set(key, "0"); // Set to 0 for consistency
                value = 0L; // Sync local fallback
                return 0L;
            } catch (KeyValueStoreException e) {
                log.error("Error resetting count in Key-Value store for key '{}'. Falling back to local counter. Error: {}",
                        key, e.getMessage());
            }
        } else {
            if (kvStoreClient == null) {
                log.trace("reset: KeyValueStoreClient is null for key '{}'. Using local counter.", key);
            } else {
                log.warn("reset: KeyValueStoreClient not connected for key '{}'. Using local counter.", key);
            }
        }
        // Fallback to local counter
        value = 0L;
        return value;
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
        if (kvStoreClient != null) {
            try {
                kvStoreClient.disconnect();
                log.info("KeyValueStoreClient disconnected for aggregator with key '{}'.", key);
            } catch (KeyValueStoreException e) {
                log.error("Error disconnecting KeyValueStoreClient for key '{}'. Error: {}", key, e.getMessage());
            } finally {
                kvStoreClient = null; // Release the client
            }
        }
    }

    @Override
    public Object[] currentState() {
        if (kvStoreClient != null && kvStoreClient.isConnected()) {
            try {
                String kvValueStr = kvStoreClient.get(key);
                if (kvValueStr != null) {
                    long kvValue = Long.parseLong(kvValueStr);
                    value = kvValue; // Sync local fallback
                    return new Object[]{new AbstractMap.SimpleEntry<>("Value", kvValue)};
                } else {
                    log.debug("Key '{}' not found in Key-Value store for currentState. Assuming 0.", key);
                    value = 0L;
                }
            } catch (NumberFormatException e) {
                log.error("Value for key '{}' in Key-Value store is not a valid Long. Falling back to local counter. Error: {}",
                        key, e.getMessage());
            } catch (KeyValueStoreException e) {
                log.error("Error reading current state from Key-Value store for key '{}'. Falling back to local counter. Error: {}",
                        key, e.getMessage());
            }
        } else {
            if (kvStoreClient == null) {
                log.trace("currentState: KeyValueStoreClient is null for key '{}'. Using local counter.", key);
            } else {
                log.warn("currentState: KeyValueStoreClient not connected for key '{}'. Using local counter.", key);
            }
        }
        // Fallback to local counter
        return new Object[]{new AbstractMap.SimpleEntry<>("Value", value)};
    }

    @Override
    public void restoreState(Object[] state) {
        Map.Entry<String, Object> stateEntry = (Map.Entry<String, Object>) state[0];
        long restoredValue = (Long) stateEntry.getValue();
        value = restoredValue;

        if (kvStoreClient != null && kvStoreClient.isConnected()) {
            try {
                kvStoreClient.set(key, String.valueOf(restoredValue));
                log.info("Successfully restored state for key '{}' to {} in Key-Value store.", key, restoredValue);
            } catch (KeyValueStoreException e) {
                log.error("Error restoring state to Key-Value store for key '{}'. State only restored to local counter. Error: {}",
                        key, e.getMessage());
            }
        } else {
            if (kvStoreClient == null) {
                log.warn("restoreState: KeyValueStoreClient is null for key '{}'. State restored to local counter only.", key);
            } else {
                log.warn("restoreState: KeyValueStoreClient not connected for key '{}'. State restored to local counter only.", key);
            }
        }
    }
}