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

package com.xbd.rocketmq.client.annotation;

import org.springframework.stereotype.Component;

import java.lang.annotation.*;

/**
 * This annotation is used over a class which implements interface
 * org.apache.rocketmq.spring.core.RocketMQLocalTransactionListener, which will be converted to
 * org.apache.rocketmq.client.producer.TransactionListener later. The class implements
 * two methods for process callback events after the txProducer sends a transactional message.
 * <p>Note: The annotation is used only on RocketMQ client producer side, it can not be used
 * on consumer side.
 */
@Target({ElementType.TYPE, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface RocketMQTransactionListener {

    String ENDPOINTS_PLACEHOLDER = "${rocketmq.endpoints:}";
    String ACCESS_KEY_PLACEHOLDER = "${rocketmq.accessKey:}";
    String SECRET_KEY_PLACEHOLDER = "${rocketmq.secretKey:}";

    String topic();

    String tag();

    String rocketMQTemplateBeanName() default "rocketMQTemplate";

    String endpoints() default ENDPOINTS_PLACEHOLDER;

    String accessKey() default ACCESS_KEY_PLACEHOLDER;

    String secretKey() default SECRET_KEY_PLACEHOLDER;

}
