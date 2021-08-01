package com.yw.middleware.example.consumer;

import com.yw.middleware.example.common.Constants;
import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * 简单的消费者
 *
 * @author yangwei
 */
public class SimpleConsumer extends ShutdownableThread {
    private KafkaConsumer<Integer, String> consumer;

    public static void main(String[] args) {
        SimpleConsumer consumer = new SimpleConsumer();
        consumer.start();
    }

    private SimpleConsumer() {
        // 第一个参数：为当前消费者指定一个名称，随意
        // 第二个参数：消费过程是否会被中断
        super("KafkaConsumerTest", false);

        Properties properties = new Properties();
        // 指定集群
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_CLUSTER_SERVER);
        // 指定消费者组名称
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "cityGroup1");
        // 指定自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // 设置自动提交的最晚时间间隔
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        // 指定消费者被认为已经挂了的时限
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        // 指定消费者发送心跳的时间间隔
        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "10000");
        // 若消费者没有指定 offset 的初始值或指定的 offset 不存在，则从这里获取 offset
        // earliest：从第一条消息开始消费
        // lastest：从最后一条消息开始消费
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 指定key与value的反序列化器
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.IntegerDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        this.consumer = new KafkaConsumer<>(properties);
    }

    @Override
    public void doWork() {
        // 指定要消费的主题，可以指定多个主题
        consumer.subscribe(Collections.singletonList("cities"));
        // poll()的参数是当前消费者等待消费的最长时间，在指定时间内没有等到消费，则返回空
        ConsumerRecords<Integer, String> records = consumer.poll(1000);
        for (ConsumerRecord record : records) {
            System.out.print("topic = " + record.topic());
            System.out.print(" partition = " + record.partition());
            System.out.print(" key = " + record.key());
            System.out.println(" value = " + record.value());
        }
    }
}
