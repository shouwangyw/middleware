package com.yw.middleware.example.producer;

import com.yw.middleware.example.common.Constants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

/**
 * 批量发送消息
 *
 * @author yangwei
 */
public class BatchProducer {
    /**
     * 第一个泛型为key的类型，第二个泛型为消息本身的类型
     */
    private KafkaProducer<Integer, String> producer;

    public static void main(String[] args) throws IOException {
        BatchProducer producer = new BatchProducer();
        producer.sendMsg();
        System.in.read();
    }

    private BatchProducer() {
        Properties properties = new Properties();
        // 指定kafka集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
//        properties.put("bootstrap.servers",
                Constants.KAFKA_CLUSTER_SERVER);
        // 指定key-value的序列化器
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
//        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
//        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        // 指定要批量发送的消息个数，默认16k
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,
//        properties.put("batch.size",
                16384);     // 16K
        // 指定积攒消息的时长，默认值为0ms
        properties.put(ProducerConfig.LINGER_MS_CONFIG,
//        properties.put("linger.ms",
                50);        // 50ms
        this.producer = new KafkaProducer<>(properties);
    }

    private void sendMsg() {
        for (int i = 0; i < 50; i++) {
            ProducerRecord<Integer, String> record =
                    new ProducerRecord<>("cities", 0, i * 10, "city-" + i * 100);
            // RecordMetadata，消息元数据，即主题、消息的key、消息本身等的封装对象
            producer.send(record, (metadata, e) -> {
                System.out.print("partition = " + metadata.partition());
                System.out.print("，topic = " + metadata.topic());
                System.out.println("，offset = " + metadata.offset());
            });
        }
    }
}
