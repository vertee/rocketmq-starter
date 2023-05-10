package com.xbd.rocketmq.client.autoconfigure;

import com.xbd.rocketmq.client.annotation.RocketMQTransactionListener;
import com.xbd.rocketmq.client.core.RocketMQTemplate;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.apis.*;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.TransactionChecker;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.aop.scope.ScopedProxyUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;

import java.util.Map;
import java.util.stream.Collectors;

@Configuration
@Slf4j
public class RocketMQTransactionConfiguration implements ApplicationContextAware, SmartInitializingSingleton {

    private ConfigurableApplicationContext applicationContext;
    private final ConfigurableEnvironment environment;
    final ClientServiceProvider provider = ClientServiceProvider.loadService();

    public RocketMQTransactionConfiguration(ConfigurableEnvironment environment) {
        this.environment = environment;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = (ConfigurableApplicationContext) applicationContext;
    }

    @Override
    public void afterSingletonsInstantiated() {
        Map<String, Object> beans = this.applicationContext.getBeansWithAnnotation(RocketMQTransactionListener.class)
            .entrySet().stream().filter(entry -> !ScopedProxyUtils.isScopedTarget(entry.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        beans.forEach(this::registerTransactionListener);
    }

    private void registerTransactionListener(String beanName, Object bean) {
        Class<?> clazz = AopProxyUtils.ultimateTargetClass(bean);

        if (!TransactionChecker.class.isAssignableFrom(bean.getClass())) {
            throw new IllegalStateException(clazz + " is not instance of " + TransactionChecker.class.getName());
        }
        RocketMQTransactionListener annotation = clazz.getAnnotation(RocketMQTransactionListener.class);
        String topic = annotation.topic();
        String tag = annotation.tag();
        String producerKey = String.format("%s_%s", topic, tag);
        RocketMQTemplate rocketMQTemplate = (RocketMQTemplate) applicationContext.getBean(annotation.rocketMQTemplateBeanName());
        if (rocketMQTemplate.transactionBeanIsExist(producerKey)) {
            throw new IllegalStateException(annotation.rocketMQTemplateBeanName() + " already exists RocketMQLocalTransactionListener");
        } else {
            String accessKey = environment.resolvePlaceholders(environment.resolvePlaceholders(annotation.accessKey()));
            String secretKey = environment.resolvePlaceholders(environment.resolvePlaceholders(annotation.secretKey()));
            String endpoints = environment.resolvePlaceholders(environment.resolvePlaceholders(annotation.endpoints()));
            SessionCredentialsProvider sessionCredentialsProvider = new StaticSessionCredentialsProvider(accessKey, secretKey);
            ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
                    .setEndpoints(endpoints)
                    .setCredentialProvider(sessionCredentialsProvider)
                    .build();
            try {
                Producer producer = provider.newProducerBuilder()
                        .setClientConfiguration(clientConfiguration)
                        .setTopics(topic)
                        .setTransactionChecker((TransactionChecker)bean)
                        .build();
                rocketMQTemplate.addTransactionBean(producerKey, producer);
            } catch (ClientException e) {
                log.error("Build rocketmq transaction producer failed, beanName={}", beanName, e);
                throw new IllegalStateException(beanName + "Build rocketmq transaction producer failed");
            }
        }
        log.debug("RocketMQLocalTransactionListener {} register to {} success", clazz.getName(), annotation.rocketMQTemplateBeanName());
    }
}
