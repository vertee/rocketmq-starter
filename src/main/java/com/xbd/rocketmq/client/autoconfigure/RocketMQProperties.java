package com.xbd.rocketmq.client.autoconfigure;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author vertee
 * @version 1.0
 * @desc 配置Bean
 */

@SuppressWarnings("WeakerAccess")
@ConfigurationProperties(prefix = "rocketmq")
@Data
public class RocketMQProperties {

    /**
     * 访问端点，可配置多个，以分号(;)隔开，如果有前置代理proxy grpc协议，请使用grpc端口（常用8081）
     */
    private String endpoints;

    /**
     * accessKey
     */
    private String accessKey;

    /**
     * secretKey
     */
    private String secretKey;

    /**
     * topic列表，producer实例化时check rocketmq是否有相应topic信息，推荐配置
     */
    private String topics;

    /**
     * 请求超时时间，默认为3秒
     */
    private Integer requestTimeout = 3;

}
