package com.uxin.commons.kafka;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uxin.commons.kafka.hessian2.MQDeserializer;

/**
 * Kafka 消费者,接收到信息后执行process方法
 *
 * @author: ellis.luo
 * @date 2016年7月28日 下午6:13:27<br/>
 *       修改为泛型by smartlv
 */
public abstract class AbstractMQConsumer<T> implements Runnable
{
    private Logger Logger = LoggerFactory.getLogger(this.getClass());

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final int COMMIT_INTERVAL = Integer.valueOf(KafkaConfig.getCommitInterval()); // offset批量提交个数
    private final int RETRY_INTERVAL = Integer.valueOf(KafkaConfig.getRetryInterval()); // 失败重试间隔时间10s

    /**
     * Topic主题,1个节点和1个Topic对应1个consumer
     */
    private String topic;

    /**
     * 是否进行重试,为true则会在遇到错误后10s进行重试 (注:1.这里可能会出现因为某些用户消息的特殊异常而导致其他用户消息。2.需要自行拦截重复消费的异常,否则会一直循环重复消费。谨慎使用!!!)
     *
     * @return
     */
    private boolean retry = false;

    private Properties props;
    private KafkaConsumer<String, MQEntry> consumer;

    // ************** 消费失败重试 **************
    private Map<String, Long> retryMes = new ConcurrentHashMap<String, Long>();

    /**
     * 执行主要业务处理流程
     *
     * @param entry
     */
    protected abstract void process(MQEntry<T> entry);

    private void init() throws Exception
    {
        Logger.info("{} init begin ", getClass().getSimpleName());
        props = new Properties();
        props.put("bootstrap.servers", KafkaConfig.getHost());
        props.put("group.id", KafkaConfig.getGroupId());
        props.put("enable.auto.commit", KafkaConfig.getEnableAutoCommit());
        props.put("auto.commit.interval.ms", KafkaConfig.getAutoCommitIntervalMs());
        props.put("max.partition.fetch.bytes", KafkaConfig.getMaxPartitionFetchBytes());
        props.put("auto.offset.reset", KafkaConfig.getAutoOffsetReset());
        props.put("heartbeat.interval.ms", KafkaConfig.getHeartBeatIntervalMs());
        props.put("session.timeout.ms", KafkaConfig.getSessionTimeout());
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", MQDeserializer.class);

        consumer = new KafkaConsumer<String, MQEntry>(props);
        Logger.info("{} init end , Properties:{}", getClass().getSimpleName(), props);

        // 解析相关注解配置
        analysisAnnotation();
    }

    private void process() throws Exception
    {
        init(); // 初始化
        subscribe(topic); // 订阅Topic
    }

    private void subscribe(String topic)
    {
        Logger.info("begin subscribe Topic:({})", topic);

        consumer.subscribe(Arrays.asList(topic));
        List<ConsumerRecord<String, MQEntry>> buffer = new ArrayList<ConsumerRecord<String, MQEntry>>();

        while (!closed.get())
        {
            ConsumerRecords<String, MQEntry> records = consumer.poll(100);

            for (ConsumerRecord<String, MQEntry> record : records)
            {

                // 检查是否需要消费失败重试
                checkRetry(record);

                Logger.info("MQ get topic = {}, offset = {}, key = {}, value = {}, partition = {},  group = {} ",
                        record.topic(), record.offset(), record.key(), new String(record.value().toString()),
                        record.partition(), KafkaConfig.getGroupId());

                try
                {
                    process(record.value());
                    buffer.add(record);

                    if (buffer.size() >= COMMIT_INTERVAL)
                    {
                        consumer.commitSync();
                        buffer.clear();
                        // 消除异常标识
                        cleanRetryMark(record);
                    }
                }
                catch (Exception ex)
                {
                    if (!closed.get())
                    {
                        Logger.error(
                                "MQ process error < topic:" + record.topic() + " offset:" + record.offset() + " key:"
                                        + record.key() + " value:" + record.value() + " partition:"
                                        + record.partition() + " group:" + KafkaConfig.getGroupId() + " >", ex);

                        // 重复消费
                        goRetry(record);
                    }
                }
            }
        }
    }

    @Override
    public void run()
    {
        try
        {
            process();
        }
        catch (Throwable t)
        {
            if (!closed.get())
            {
                Logger.error("MQ Consumer error ", t);
                subscribe(topic);
            }
        }
    }

    // Shutdown hook which can be called from a separate thread
    protected void shutdown()
    {
        closed.set(true);
        consumer.wakeup();
    }

    private void seekPosition(ConsumerRecord<String, MQEntry> record)
    {
        long errorOffset = (retryMes.get(String.valueOf(record.partition())) == null ? 0 : retryMes.get(String
                .valueOf(record.partition())));

        // 如果有多个报错重试的offset进来,取最小offset位置的进行重试消费
        if (errorOffset == 0 || errorOffset > record.offset())
        {
            retryMes.put(String.valueOf(record.partition()), record.offset());
        }

        // 上面put了,刷新一次
        errorOffset = (retryMes.get(String.valueOf(record.partition())) == null ? 0 : retryMes.get(String
                .valueOf(record.partition())));

        TopicPartition p = new TopicPartition(record.topic(), record.partition());
        consumer.seek(p, errorOffset);

        Logger.info("seekPosition partition:{}, errorOffset:{}, recordOffset:{}", record.partition(), errorOffset,
                record.offset());
    }

    private void checkRetry(ConsumerRecord<String, MQEntry> record)
    {
        if (retry && KafkaConfig.getRetrySwitch().equals("on"))
        {
            long errorOffset = (retryMes.get(String.valueOf(record.partition())) == null ? 0 : retryMes.get(String
                    .valueOf(record.partition())));

            Logger.info("checkRetry partition:{}, errorOffset:{}, recordOffset:{}", record.partition(), errorOffset,
                    record.offset());

            if (errorOffset == 0) // 没异常重试 or 重启的时候
            {
                return;
            }
            else if (errorOffset != record.offset()) // 当已经存在一个异常消费时,后面会重新定位到错误的offset,无法进行消费
            {
                if (errorOffset < record.offset())
                {
                    seekPosition(record);
                }
            }
            else if (errorOffset == record.offset()) // 重复消费错误异常
            {
                // 抛出异常并且同时设置retry为true的时候,会每隔10s进行重复消费
                if (errorOffset > 0)
                {
                    try
                    {
                        TimeUnit.SECONDS.sleep(RETRY_INTERVAL);
                    }
                    catch (InterruptedException e)
                    {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private void goRetry(ConsumerRecord<String, MQEntry> record)
    {
        if (retry && KafkaConfig.getRetrySwitch().equals("on"))
        {
            // 重新消费
            seekPosition(record);
        }
    }

    private void cleanRetryMark(ConsumerRecord<String, MQEntry> record)
    {
        if (retry && KafkaConfig.getRetrySwitch().equals("on"))
        {
            // 消除重试标识
            retryMes.remove(String.valueOf(record.partition()));
        }
    }

    private void analysisAnnotation() throws Exception
    {
        Method method = Class.forName(getClass().getName()).getDeclaredMethod("process", MQEntry.class);
        KafkaAnnotation kafkaAnnotation = method.getAnnotation(KafkaAnnotation.class);
        topic = kafkaAnnotation.topic();
        retry = kafkaAnnotation.retry();
    }

    public static void main(String[] args)
    {
    }

}
