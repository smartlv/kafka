package com.uxin.commons.kafka;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

/**
 * MQ 线程管理器
 * 
 * @author ellis.luo
 * @date 2016年8月15日 上午11:10:11
 */
@Component
public class MQConsumerManager implements ApplicationListener<ApplicationEvent>
{
    private Logger Logger = LoggerFactory.getLogger(MQConsumerManager.class);

    @Autowired
    private List<AbstractMQConsumer> mqs;

    private static boolean isStart = false;

    @Override
    public void onApplicationEvent(ApplicationEvent event)
    {
        if (!isStart)
        {
            Logger.info("MQConsumerManager init");
            isStart = true;

            ExecutorService s = Executors.newFixedThreadPool(mqs.size());
            for (AbstractMQConsumer mq : mqs)
            {
                s.execute(mq);
            }

        }
    }

}
