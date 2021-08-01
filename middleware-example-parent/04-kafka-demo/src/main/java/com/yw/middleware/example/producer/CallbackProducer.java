package com.yw.middleware.example.producer;

import com.yw.middleware.example.common.Constants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

/**
 * 发送可以回调的消息
 *
 * @author yangwei
 */
public class CallbackProducer {
    /**
     * 第一个泛型为key的类型，第二个泛型为消息本身的类型
     */
    private KafkaProducer<Integer, String> producer;

    public static void main(String[] args) throws IOException {
        CallbackProducer producer = new CallbackProducer();
        producer.sendMsg();
        System.in.read();
    }

    private CallbackProducer() {
        Properties properties = new Properties();
        // 指定kafka集群
        properties.put("bootstrap.servers", Constants.KAFKA_CLUSTER_SERVER);
        // 指定key-value的序列化器
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

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
                new ProducerRecord<>("cities", 0, 1, "shanghai");

        // 可以调用以下两个参数的send()方法，可以在消息发布成功后触发回调的执行
        // metadata: RecordMetadata，消息元数据，即主题、消息的key、消息本身等的封装对象
        producer.send(record, (metadata, exception) -> {
            System.out.print("partition = " + metadata.partition());
            System.out.print("，topic = " + metadata.topic());
            System.out.println("，offset = " + metadata.offset());
        });
    }
}
