package com.yw.middleware.example.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * @author yangwei
 */
@Component
public class SimpleConsumer {
    @KafkaListener(topics = "${kafka.topic}")
    public void recvMsg(String message) {
        System.out.println("Kafka消费者接受到消息：" + message);
    }
}