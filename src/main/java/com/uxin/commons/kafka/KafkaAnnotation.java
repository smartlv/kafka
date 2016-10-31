package com.uxin.commons.kafka;

import java.lang.annotation.*;

/**
 * Kafka注解配置 xiaowei.luo
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface KafkaAnnotation
{
    String topic() default ""; // Kafka Topic

    boolean retry() default false; // 是否重试消费,默认为false,为true则会在遇到错误后10s进行重试
                                   // (注:1.这里可能会出现因为某些用户消息的特殊异常而导致其他用户消息。2.需要自行拦截重复消费的异常,否则会一直循环重复消费。3.只建议记录日志类型的操作打开retry。谨慎使用!!!)
}
