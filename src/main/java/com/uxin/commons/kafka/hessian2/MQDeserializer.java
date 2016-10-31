package com.uxin.commons.kafka.hessian2;

import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import com.uxin.commons.kafka.MQEntry;
import com.uxin.commons.serialization.Hessian2Serialization;

/**
 * 实现kafka Deserializer接口的反序列化
 * 
 * @author: ellis.luo
 * @date 2016年7月28日 下午6:13:27
 */
public class MQDeserializer implements Deserializer<MQEntry>
{

    /**
     * 初始化配置
     * 
     * @param configs
     *        配置信息
     * @param isKey
     *        是key还是value
     */
    public void configure(Map<String, ?> configs, boolean isKey)
    {
    }

    /**
     * 反序列化方法
     * 
     * @param topic
     *        主题名字
     * @param data
     *        字节数组
     * @return 事件对象
     * @throws SerializationException
     *         序列化异常
     */
    public MQEntry deserialize(String topic, byte[] data)
    {
        try
        {
            return Hessian2Serialization.deserialize(data, MQEntry.class);
        }
        catch (Exception e)
        {
            throw new SerializationException(topic + " JsonConverter deserializer error", e);
        }
    }

    /**
     * 关闭
     */
    public void close()
    {

    }
}
