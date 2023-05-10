package com.xbd.rocketmq.client.support;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageBuilder;
import org.apache.rocketmq.shaded.commons.lang3.ArrayUtils;
import org.apache.rocketmq.shaded.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * @Author vertee
 * @Date 2023/3/22 10:58
 */
public class RocketMQUtil {

    final static ClientServiceProvider provider = ClientServiceProvider.loadService();

    public static Message convertToRocketMessage(String topic, Object payloadObj, String messageGroup, Duration delay, String tag, String... keys) {
        MessageBuilder builder = provider.newMessageBuilder().setTopic(topic).setBody(getMsgBody(payloadObj));
        if (StringUtils.isNotBlank(messageGroup)) {
            builder.setMessageGroup(messageGroup);
        }
        if (delay != null) {
            builder.setDeliveryTimestamp(System.currentTimeMillis() + delay.toMillis());
        }
        if (StringUtils.isNotBlank(tag)) {
            builder.setTag(tag);
        }
        if (ArrayUtils.isNotEmpty(keys)) {
            builder.setKeys(keys);
        }
        return builder.build();
    }

    private static byte[] getMsgBody(Object payload) {
        byte[] payloads;
        if (payload instanceof String) {
            payloads = ((String) payload).getBytes(StandardCharsets.UTF_8);
        } else if (payload instanceof byte[]) {
            payloads = (byte[]) payload;
        } else {
            payloads = JSON.toJSONBytes(payload);
        }
        return payloads;
    }

}
