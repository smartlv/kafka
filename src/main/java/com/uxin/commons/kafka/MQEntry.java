package com.uxin.commons.kafka;

import java.io.Serializable;

import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * MQ消息的包体
 * 
 * @author: ellis.luo
 * @date 2016年7月28日 下午6:13:27</br> <br/>
 *       修改为泛型by smartlv
 */
public class MQEntry<T> implements Serializable
{
    private static final long serialVersionUID = 7485793906662475274L;

    /**
     * MQ消息的业务Topic，没有分类的情况下可以和MQ的Topic相同
     */
    public String topic;
    /**
     * MQ消息的包体信息，需要实现Serializable接口
     */
    public T body;

    public MQEntry(String topic, T body)
    {
        this.topic = topic;
        this.body = body;
    }

    public T getBody()
    {
        return body;
    }

    public void setBody(T body)
    {
        this.body = body;
    }

    public String getTopic()
    {
        return topic;
    }

    public void setTopic(String topic)
    {
        this.topic = topic;
    }

    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this);
    }

}
