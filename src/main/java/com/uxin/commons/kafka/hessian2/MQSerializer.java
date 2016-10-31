package com.uxin.commons.kafka.hessian2;

import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import com.uxin.commons.serialization.Hessian2Serialization;

/**
 * 实现kafka Serializer接口的序列化
 * 
 * @author: ellis.luo
 * @date 2016年7月28日 下午6:13:27
 */
public class MQSerializer implements Serializer<Object>
{

    public void configure(Map configs, boolean isKey)
    {
    }

    public byte[] serialize(String topic, Object data)
    {
        try
        {
            return Hessian2Serialization.serialize(data);
        }
        catch (Exception e)
        {
            throw new SerializationException(topic + " JsonConverter serializer error", e);
        }
    }

    public void close()
    {
        // nothing to do
    }
}
