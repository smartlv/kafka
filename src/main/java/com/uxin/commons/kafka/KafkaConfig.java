package com.uxin.commons.kafka;

import com.baidu.disconf.client.common.annotations.DisconfFile;
import com.baidu.disconf.client.common.annotations.DisconfFileItem;
import org.springframework.stereotype.Service;

/**
 * kafka配置
 * 
 * @author ellis.luo
 * @date 2016年8月8日 下午12:16:59
 */
@Service
@DisconfFile(filename = "kafka.properties")
public class KafkaConfig
{
    // consumer
    private static String host = "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094";
    private static String groupId = "hds";
    private static String heartBeatIntervalMs = "10000";
    private static String sessionTimeout = "30000";
    private static String maxPartitionFetchBytes = "4500";
    private static String enableAutoCommit = "false";
    private static String autoCommitIntervalMs = "1000";
    private static String autoOffsetReset = "latest";
    private static String commitInterval = "1";
    private static String retryInterval = "10";// 重试间隔时间
    private static String retrySwitch = "on"; // 重试开关

    @DisconfFileItem(name = "kafka.host")
    public static String getHost()
    {
        return host;
    }

    @DisconfFileItem(name = "kafka.group")
    public static String getGroupId()
    {
        return groupId;
    }

    @DisconfFileItem(name = "kafka.heartbeat.interval.ms")
    public static String getHeartBeatIntervalMs()
    {
        return heartBeatIntervalMs;
    }

    @DisconfFileItem(name = "kafka.session.timeout")
    public static String getSessionTimeout()
    {
        return sessionTimeout;
    }

    @DisconfFileItem(name = "kafka.max.partition.fetch.bytes")
    public static String getMaxPartitionFetchBytes()
    {
        return maxPartitionFetchBytes;
    }

    @DisconfFileItem(name = "kafka.enable.auto.commit")
    public static String getEnableAutoCommit()
    {
        return enableAutoCommit;
    }

    @DisconfFileItem(name = "kafka.auto.commit.intervals.ms")
    public static String getAutoCommitIntervalMs()
    {
        return autoCommitIntervalMs;
    }

    @DisconfFileItem(name = "kafka.auto.offset.reset")
    public static String getAutoOffsetReset()
    {
        return autoOffsetReset;
    }

    @DisconfFileItem(name = "kafka.offset.commit.interval")
    public static String getCommitInterval()
    {
        return commitInterval;
    }

    @DisconfFileItem(name = "kafka.offset.retry.interval")
    public static String getRetryInterval()
    {
        return retryInterval;
    }

    @DisconfFileItem(name = "kafka.offset.retry.switch")
    public static String getRetrySwitch()
    {
        return retrySwitch;
    }

}
