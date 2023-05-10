package com.xbd.rocketmq.client;

import com.xbd.rocketmq.client.autoconfigure.ListenerContainerConfiguration;
import com.xbd.rocketmq.client.autoconfigure.RocketMQListenerConfiguration;
import com.xbd.rocketmq.client.autoconfigure.RocketMQProperties;
import com.xbd.rocketmq.client.autoconfigure.RocketMQTransactionConfiguration;
import com.xbd.rocketmq.client.core.RocketMQTemplate;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.apis.*;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.ProducerBuilder;
import org.apache.rocketmq.shaded.commons.lang3.StringUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import java.time.Duration;

/**
 * @author jibaole
 * @version 1.0
 * @desc 初始化(生成|消费)相关配置
 * @date 2018/7/7 下午5:19
 */
@Configuration
@EnableConfigurationProperties(RocketMQProperties.class)
@Import({ListenerContainerConfiguration.class, RocketMQListenerConfiguration.class, RocketMQTransactionConfiguration.class})
@Slf4j
public class RocketMQAutoConfiguration implements ApplicationContextAware {

    @Autowired
    private RocketMQProperties config;

    private ApplicationContext applicationContext;

    final ClientServiceProvider provider = ClientServiceProvider.loadService();

    public static final String ROCKETMQ_TEMPLATE_GLOBAL_NAME = "rocketMQTemplate";
    public static final String DEFAULT_PRODUCER_BEAN_NAME = "defaultMQProducer";

    @PostConstruct
    public void checkProperties() {
        log.debug("rocketmq.endpoints = {}", config.getEndpoints());
        if (StringUtils.isBlank(config.getEndpoints())) {
            log.warn("The necessary spring property 'rocketmq.endpoints' is not defined, all rocketMq beans creation are skipped!");
        }
    }

    @Bean(name = DEFAULT_PRODUCER_BEAN_NAME, destroyMethod = "close")
    @ConditionalOnMissingBean(name = DEFAULT_PRODUCER_BEAN_NAME)
    public Producer producer() throws ClientException {
        log.info("Rocketmq default producer init……");
        Assert.hasText(config.getEndpoints(), "[rocketmq.endpoints] must not be null");
        Assert.hasText(config.getAccessKey(), "[rocketmq.accessKey] must not be null");
        Assert.hasText(config.getSecretKey(), "[rocketmq.secretKey] must not be null");
        SessionCredentialsProvider sessionCredentialsProvider = new StaticSessionCredentialsProvider(config.getAccessKey(), config.getSecretKey());

        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
                .setEndpoints(config.getEndpoints())
                .setCredentialProvider(sessionCredentialsProvider)
                .setRequestTimeout(Duration.ofSeconds(config.getRequestTimeout()))
                .build();
        ProducerBuilder builder = provider.newProducerBuilder().setClientConfiguration(clientConfiguration);
        if (config.getTopics() != null && config.getTopics().length() > 0) {
            builder.setTopics(config.getTopics().split(","));
        }
        return builder.build();
    }

    @Bean(destroyMethod = "destroy")
    @ConditionalOnMissingBean(name = ROCKETMQ_TEMPLATE_GLOBAL_NAME)
    public RocketMQTemplate rocketMQTemplate() throws ClientException {
        RocketMQTemplate rocketMQTemplate = new RocketMQTemplate();
        rocketMQTemplate.setProducer(producer());
        return rocketMQTemplate;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

}
