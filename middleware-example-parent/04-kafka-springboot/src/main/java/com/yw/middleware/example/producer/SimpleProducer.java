package com.yw.middleware.example.producer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * @author yangwei
 */
@RestController("/msg")
public class SimpleProducer {
    @Resource
    private KafkaTemplate<String, String> template;

    /**
     * 从配置文件读取自定义属性
     */
    @Value("${kafka.topic}")
    private String topic;

    /**
     * 由于是提交数据，所以使用Post方式
     */
    @PostMapping("send")
    public String sendMsg(@RequestParam("message") String message) {
        template.send(topic, message);
        return "send success";
    }
}