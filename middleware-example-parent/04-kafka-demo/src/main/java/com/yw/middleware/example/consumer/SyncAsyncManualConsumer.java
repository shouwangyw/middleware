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
 * 同异步提交
 *
 * @author yangwei
 */
public class SyncAsyncManualConsumer extends ShutdownableThread {
    private KafkaConsumer<Integer, String> consumer;

    public static void main(String[] args) {
        SyncAsyncManualConsumer consumer = new SyncAsyncManualConsumer();
        consumer.start();
    }

    private SyncAsyncManualConsumer() {
        super("KafkaConsumerTest", false);

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_CLUSTER_SERVER);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "cityGroup1");
        // 指定使用手动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        // 设置一次提交的offset个数
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
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

            try {
                // 带回调功能的手动异步提交
                consumer.commitAsync((offsets, e) -> {
                    if (e != null) {
                        System.out.print("提交失败，offsets = " + offsets);
                        System.out.println("，exception = " + e);
                    }
                });
            } catch (Exception e) {
                // 同步提交
                consumer.commitSync();
            }
        }
    }
}
