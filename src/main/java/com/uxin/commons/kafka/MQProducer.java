package com.uxin.commons.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uxin.commons.kafka.hessian2.MQSerializer;

/**
 * Kafka 生产者
 *
 * @author: ellis.luo
 * @date 2016年7月28日 下午6:13:27
 */
public class MQProducer
{
    private static Logger Logger = LoggerFactory.getLogger(MQProducer.class);

    private static Properties props;
    private static KafkaProducer<String, MQEntry> producer;

    public MQProducer()
    {
    }

    public static void init()
    {
        props = new Properties();

        props.put("bootstrap.servers", "101.200.196.99:9092");
        props.put("acks", "1");
        props.put("retries", 3);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("value.serializer", MQSerializer.class);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<String, MQEntry>(props);
    }

    /**
     * 发送MQ消息
     *
     * @param topic
     *        MQ消息的Topic
     * @param MQEntry
     *        MQ消息的包体
     */
    public static void send(final String group, final MQEntry entry)
    {
        if (producer == null)
        {
            init();
        }

        ProducerRecord<String, MQEntry> record = new ProducerRecord<String, MQEntry>(entry.getTopic(), entry);
        producer.send(
                record,
                (metadata, e) -> Logger.info("MQ send topic:{}, offset:{}, entry:{} OK!", entry.getTopic(),
                        metadata.offset(), entry, e));
    }

    public static void main(String[] args)
    {
        for (;;)
        {
            send("trss", new MQEntry("smartlv", "ddd"));
        }
    }
}
