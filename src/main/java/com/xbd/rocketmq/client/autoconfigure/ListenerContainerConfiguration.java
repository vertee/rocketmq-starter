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

package com.xbd.rocketmq.client.autoconfigure;

import com.xbd.rocketmq.client.annotation.RocketMQMessageListener;
import com.xbd.rocketmq.client.core.RocketMQListener;
import com.xbd.rocketmq.client.support.DefaultRocketMQListenerContainer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.beans.BeansException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.util.StringUtils;

import java.util.concurrent.atomic.AtomicLong;

@Configuration
@Slf4j
@EnableConfigurationProperties({RocketMQProperties.class})
public class ListenerContainerConfiguration implements ApplicationContextAware {

    private ConfigurableApplicationContext applicationContext;

    private final AtomicLong counter = new AtomicLong(0);

    private final ConfigurableEnvironment environment;

    private final RocketMQProperties rocketMQProperties;

    public ListenerContainerConfiguration(ConfigurableEnvironment environment, RocketMQProperties rocketMQProperties) {
        this.environment = environment;
        this.rocketMQProperties = rocketMQProperties;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = (ConfigurableApplicationContext) applicationContext;
    }

    public void registerContainer(String beanName, Object bean, RocketMQMessageListener annotation) {
        Class<?> clazz = AopProxyUtils.ultimateTargetClass(bean);

        if (!RocketMQListener.class.isAssignableFrom(bean.getClass())) {
            throw new IllegalStateException(clazz + " is not instance of " + RocketMQListener.class.getName());
        }

        String containerBeanName = String.format("%s_%s", DefaultRocketMQListenerContainer.class.getName(), counter.incrementAndGet());
        GenericApplicationContext genericApplicationContext = (GenericApplicationContext) applicationContext;

        genericApplicationContext.registerBean(containerBeanName,
            DefaultRocketMQListenerContainer.class,
            () -> createRocketMQListenerContainer(containerBeanName, bean, annotation)
        );
        DefaultRocketMQListenerContainer container = genericApplicationContext.getBean(containerBeanName, DefaultRocketMQListenerContainer.class);
        if (!container.isRunning()) {
            try {
                container.start();
            } catch (Exception e) {
                log.error("Started container failed. {}", container, e);
                throw new RuntimeException(e);
            }
        }

        log.info("Register the listener to container, listenerBeanName:{}, containerBeanName:{}", beanName, containerBeanName);
    }

    private DefaultRocketMQListenerContainer createRocketMQListenerContainer(String name, Object bean, RocketMQMessageListener annotation) {
        DefaultRocketMQListenerContainer container = new DefaultRocketMQListenerContainer();

        container.setRocketMQMessageListener(annotation);

        String endpoints = environment.resolvePlaceholders(annotation.endpoints());
        endpoints = StringUtils.hasLength(endpoints) ? endpoints : rocketMQProperties.getEndpoints();
        container.setEndPoints(endpoints);
        container.setAccessKey(environment.resolvePlaceholders(environment.resolvePlaceholders(annotation.accessKey())));
        container.setSecretKey(environment.resolvePlaceholders(environment.resolvePlaceholders(annotation.secretKey())));
        container.setRequestTimeout(Integer.valueOf(environment.resolvePlaceholders(environment.resolvePlaceholders(annotation.requestTimeout()))));

        if (RocketMQListener.class.isAssignableFrom(bean.getClass())) {
            container.setRocketMQListener((RocketMQListener) bean);
        } else {
            log.warn("Consumer need implement RocketMQListener. {}", container);
            throw new RuntimeException("Consumer need implement RocketMQListener");
        }
        container.setName(name);
        return container;
    }

}
