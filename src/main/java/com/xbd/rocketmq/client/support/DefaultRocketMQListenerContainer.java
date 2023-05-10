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

package com.xbd.rocketmq.client.support;

import com.xbd.rocketmq.client.annotation.RocketMQMessageListener;
import com.xbd.rocketmq.client.core.RocketMQListener;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.apis.*;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.util.Assert;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Objects;

@Slf4j
public class DefaultRocketMQListenerContainer implements InitializingBean, RocketMQListenerContainer, SmartLifecycle, ApplicationContextAware {

    private ApplicationContext applicationContext;
    private boolean running;
    private String name;
    private String endPoints;
    private String accessKey;
    private String secretKey;
    private Integer requestTimeout;
    private String consumerGroup;
    private String topic;
    private RocketMQListener rocketMQListener;
    private RocketMQMessageListener rocketMQMessageListener;
    private PushConsumer consumer;
    private FilterExpressionType selectorType;
    private String tag;
    private String instanceName;
    final ClientServiceProvider provider = ClientServiceProvider.loadService();

    public void setRocketMQMessageListener(RocketMQMessageListener anno) {
        this.rocketMQMessageListener = anno;
        this.consumerGroup = anno.consumerGroup();
        this.selectorType = anno.selectorType();
        this.tag = anno.tag();
        this.instanceName = anno.instanceName();
        this.topic = anno.topic();
    }

    private void initRocketMQPushConsumer() {
        if (rocketMQListener == null) {
            throw new IllegalArgumentException("Property 'rocketMQListener' or 'rocketMQReplyListener' is required");
        }
        Assert.notNull(consumerGroup, "Property 'consumerGroup' is required");
        Assert.notNull(endPoints, "Property 'endpoint' is required");
        Assert.notNull(topic, "Property 'topic' is required");
    }

    @Override
    public void start() {
        if (this.isRunning()) {
            throw new IllegalStateException("container already running. " + this.toString());
        }
        try {
            SessionCredentialsProvider sessionCredentialsProvider =  new StaticSessionCredentialsProvider(accessKey, secretKey);
            ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
                    .setEndpoints(endPoints)
                    .setCredentialProvider(sessionCredentialsProvider)
                    .setRequestTimeout(Duration.ofSeconds(requestTimeout))
                    .build();
            FilterExpression filterExpression = new FilterExpression(this.tag, this.selectorType);
            this.consumer = provider.newPushConsumerBuilder().setClientConfiguration(clientConfiguration)
                .setConsumerGroup(consumerGroup)
                .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
                .setMessageListener(rocketMQListener).build();
        } catch (ClientException e) {
            throw new IllegalStateException("Failed to start RocketMQ push consumer", e);
        }
        this.setRunning(true);
        log.info("running container: {}", this.toString());
    }

    @Override
    public void stop() {
        if (this.isRunning()) {
            if (Objects.nonNull(consumer)) {
                try {
                    consumer.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            setRunning(false);
        }
    }

    @Override
    public void stop(Runnable callback) {
        stop();
        callback.run();
    }


    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        initRocketMQPushConsumer();
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public void destroy() throws IOException {
        this.setRunning(false);
        if (Objects.nonNull(consumer)) {
            consumer.close();
        }
        log.info("container destroyed, {}", this.toString());
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEndPoints() {
        return endPoints;
    }

    public void setEndPoints(String endPoints) {
        this.endPoints = endPoints;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public RocketMQListener getRocketMQListener() {
        return rocketMQListener;
    }

    public void setRocketMQListener(RocketMQListener rocketMQListener) {
        this.rocketMQListener = rocketMQListener;
    }

    public RocketMQMessageListener getRocketMQMessageListener() {
        return rocketMQMessageListener;
    }

    public PushConsumer getConsumer() {
        return consumer;
    }

    public void setConsumer(PushConsumer consumer) {
        this.consumer = consumer;
    }

    public FilterExpressionType getSelectorType() {
        return selectorType;
    }

    public void setSelectorType(FilterExpressionType selectorType) {
        this.selectorType = selectorType;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public Integer getRequestTimeout() {
        return requestTimeout;
    }

    public void setRequestTimeout(Integer requestTimeout) {
        this.requestTimeout = requestTimeout;
    }

}
