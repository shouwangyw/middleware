package com.yw.middleware.example.producer;

import com.yw.middleware.example.common.Constants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

/**
 * 简单发送消息
 *
 * @author yangwei
 */
public class SimpleProducer {
    /**
     * 第一个泛型为key的类型，第二个泛型为消息本身的类型
     */
    private KafkaProducer<Integer, String> producer;

    public static void main(String[] args) throws IOException {
        SimpleProducer producer = new SimpleProducer();
        producer.sendMsg();
        System.in.read();
    }

    private SimpleProducer() {
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
        // 开启Gzip压缩
        properties.put("compression.type", "gzip");
        this.producer = new KafkaProducer<>(properties);
    }

    private void sendMsg() {
        // 创建记录（消息）
        // 指定主题及消息本身
        // ProducerRecord<Integer, String> record =
        //                        new ProducerRecord<>("cities", "shanghai");
        // 指定主题、key，及消息本身
        // ProducerRecord<Integer, String> record =
        //                        new ProducerRecord<>("cities", 1, "shanghai");
        // 指定主题、要写入的patition、key，及消息本身
        ProducerRecord<Integer, String> record =
                new ProducerRecord<>("cities", 1, 1, "shanghai");

        // 发布消息，其返回值为Future对象，表示其发送过程为异步，不过这里不使用该返回结果
        // Future<RecordMetadata> future = producer.send(record);
        producer.send(record);
    }

}