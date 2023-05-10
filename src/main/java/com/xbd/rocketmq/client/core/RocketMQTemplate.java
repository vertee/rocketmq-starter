/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.xbd.rocketmq.client.core;


import com.xbd.rocketmq.client.support.RocketMQUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.apis.producer.Transaction;
import org.springframework.beans.factory.DisposableBean;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class RocketMQTemplate implements DisposableBean {

    /**
     * default producer
     */
    private Producer producer;

    /**
     * transaction producer map
     */
    private final static Map<String, Producer> TRANSACTION_PRODUCER_MAP = new ConcurrentHashMap<>();

    public <T> SendReceipt sendNormal(String topic, T payload) throws ClientException {
        return producer.send(RocketMQUtil.convertToRocketMessage(topic, payload, null, null, null));
    }

    public <T> SendReceipt sendNormal(String topic, T payload, String tag) throws ClientException {
        return producer.send(RocketMQUtil.convertToRocketMessage(topic, payload, null, null, tag));
    }

    public <T> SendReceipt sendNormal(String topic, T payload, String tag, String... keys) throws ClientException {
        return producer.send(RocketMQUtil.convertToRocketMessage(topic, payload, null, null, tag, keys));
    }

    public <T> SendReceipt sendDelay(String topic, T payload, Duration delay) throws ClientException {
        return producer.send(RocketMQUtil.convertToRocketMessage(topic, payload, null, delay, null));
    }

    public <T> SendReceipt sendDelay(String topic, T payload, Duration delay, String tag) throws ClientException {
        return producer.send(RocketMQUtil.convertToRocketMessage(topic, payload, null, delay, tag));
    }

    public <T> SendReceipt sendDelay(String topic, T payload, Duration delay, String tag, String... keys) throws ClientException {
        return producer.send(RocketMQUtil.convertToRocketMessage(topic, payload, null, delay, tag, keys));
    }

    public <T> SendReceipt sendFifo(String topic, T payload, String messageGroup) throws ClientException {
        return producer.send(RocketMQUtil.convertToRocketMessage(topic, payload, messageGroup, null, null));
    }

    public <T> SendReceipt sendFifo(String topic, T payload, String messageGroup, String tag) throws ClientException {
        return producer.send(RocketMQUtil.convertToRocketMessage(topic, payload, messageGroup, null, tag));
    }

    public <T> SendReceipt sendFifo(String topic, T payload, String messageGroup, String tag, String... keys) throws ClientException {
        return producer.send(RocketMQUtil.convertToRocketMessage(topic, payload, messageGroup, null, tag, keys));
    }

    public boolean transactionBeanIsExist(String name) {
        return TRANSACTION_PRODUCER_MAP.containsKey(name);
    }

    public void addTransactionBean(String name, Producer producer) {
        TRANSACTION_PRODUCER_MAP.put(name, producer);
    }

    public Transaction beginTransaction(String topic, String tag) throws ClientException {
        String key = String.format("%s_%s", topic, tag);
        if (TRANSACTION_PRODUCER_MAP.containsKey(key)) {
            return TRANSACTION_PRODUCER_MAP.get(key).beginTransaction();
        } else {
            throw new ClientException("Transaction bean is not exist, please check annotation RocketMQTransactionListener");
        }
    }

    public <T> SendReceipt sendTransaction(Transaction transaction, String topic, T payload, String tag) throws ClientException {
        String key = String.format("%s_%s", topic, tag);
        if (TRANSACTION_PRODUCER_MAP.containsKey(key)) {
            return TRANSACTION_PRODUCER_MAP.get(key).send(RocketMQUtil.convertToRocketMessage(topic, payload, null, null, tag), transaction);
        } else {
            throw new ClientException("Transaction producer is not register, please check your configuration");
        }
    }

    public <T> SendReceipt sendTransaction(Transaction transaction, String topic, T payload, String tag, String... keys) throws ClientException {
        String key = String.format("%s_%s", topic, tag);
        if (TRANSACTION_PRODUCER_MAP.containsKey(key)) {
            return TRANSACTION_PRODUCER_MAP.get(key).send(RocketMQUtil.convertToRocketMessage(topic, payload, null, null, tag, keys), transaction);
        } else {
            throw new ClientException("Transaction producer is not register, please check your configuration");
        }
    }

    @Override
    public void destroy() throws Exception {
        if (producer != null) {
            producer.close();
        }
        if (!TRANSACTION_PRODUCER_MAP.isEmpty()) {
            TRANSACTION_PRODUCER_MAP.values().forEach(p -> {
                try {
                    p.close();
                } catch (IOException e) {
                    log.warn("Transaction producer close failed...");
                }
            });
        }
    }

    public Producer getProducer() {
        return producer;
    }

    public void setProducer(Producer producer) {
        this.producer = producer;
    }

}