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
import org.wso2.siddhi.query.api.definition.Attribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.Map;

public class CountAttributeAggregator extends AttributeAggregator {

    private static Attribute.Type type = Attribute.Type.LONG;
    private long value = 0l;
    private static final Logger log = LoggerFactory.getLogger(CountAttributeAggregator.class);
    private String key;

    /**
     * The initialization method for FunctionExecutor
     *
     * @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param executionPlanContext         Execution plan runtime context
     */
    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        this.key = QuerySelector.getThreadLocalGroupByKey();
    }

    public Attribute.Type getReturnType() {
        return type;
    }

    @Override
    public Object processAdd(Object data) {
        long threadId = Thread.currentThread().getId();
        String threadName = Thread.currentThread().getName();

        log.info("pcAdd START - Key: {}, Thread: {} ({}), Instance: {}, time: {}",
                key, threadId, threadName, System.identityHashCode(this),System.currentTimeMillis());
        try {
            Thread.sleep(1000); // Wait for 1 second
            value++;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore the interrupted status
            throw new RuntimeException("Thread interrupted during sleep", e);
        } finally {
            log.info("pcAdd END - Key: {}, Thread: {} ({}), Instance: {}, time: {}",
                    key, threadId, threadName, System.identityHashCode(this), System.currentTimeMillis());
        }
        return value;
    }

    @Override
    public Object processAdd(Object[] data) {
        value++;
        return value;
    }

    @Override
    public Object processRemove(Object data) {
        long threadId = Thread.currentThread().getId();
        String threadName = Thread.currentThread().getName();

        log.info("pcRemove START - Key: {}, Thread: {} ({}), Instance: {}, time: {}",
                key, threadId, threadName, System.identityHashCode(this),System.currentTimeMillis());
        try {
            Thread.sleep(1000); // Wait for 1 second
            value--;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore the interrupted status
            throw new RuntimeException("Thread interrupted during sleep", e);
        } finally {
            log.info("pcRemove END - Key: {}, Thread: {} ({}), Instance: {}, time: {}",
                    key, threadId, threadName, System.identityHashCode(this), System.currentTimeMillis());
        }
        return value;
    }

    @Override
    public Object processRemove(Object[] data) {
        value--;
        return value;
    }

    @Override
    public Object reset() {
        value = 0l;
        return value;
    }

    @Override
    public void start() {
        //Nothing to start
    }

    @Override
    public void stop() {
        //nothing to stop
    }

    @Override
    public Object[] currentState() {
        return new Object[]{new AbstractMap.SimpleEntry<String, Object>("Value", value)};
    }

    @Override
    public void restoreState(Object[] state) {
        Map.Entry<String, Object> stateEntry = (Map.Entry<String, Object>) state[0];
        value = (Long) stateEntry.getValue();
    }
}
