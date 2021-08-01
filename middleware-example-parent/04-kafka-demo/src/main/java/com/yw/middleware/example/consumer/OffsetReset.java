package com.yw.middleware.example.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 位移重放
 *
 * @Author 01399565
 * @create 2020/11/19 10:00
 */
public class OffsetReset {
    public static void main(String[] args) {
        Properties properties = new Properties();
        // 要重设位移的 Kafka 主题
        String topic = "test";
        try (final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singleton(topic));
            consumer.poll(0);
            // Earliest实现
            consumer.seekToBeginning(consumer.partitionsFor(topic).stream().map(partitionInfo ->
                    new TopicPartition(topic, partitionInfo.partition())).collect(Collectors.toList()));
            // Latest实现
            consumer.seekToEnd(consumer.partitionsFor(topic).stream().map(partitionInfo ->
                    new TopicPartition(topic, partitionInfo.partition())).collect(Collectors.toList()));
            // Current实现
            consumer.partitionsFor(topic).stream().map(partitionInfo ->
                    new TopicPartition(topic, partitionInfo.partition()))
                    .forEach(topicPartition -> {
                        long offset = consumer.committed(topicPartition).offset();
                        consumer.seek(topicPartition, offset);
                    });
            // Specified-Offset实现
            long targetOffset = 1234L;
            for (PartitionInfo info : consumer.partitionsFor(topic)) {
                TopicPartition topicPartition = new TopicPartition(topic, info.partition());
                consumer.seek(topicPartition, targetOffset);
            }
            // Shift-By-N 实现
            for (PartitionInfo info : consumer.partitionsFor(topic)) {
                TopicPartition topicPartition = new TopicPartition(topic, info.partition());
                long offset = consumer.committed(topicPartition).offset() + 123L;
                consumer.seek(topicPartition, offset);
            }
            // datetime实现
            long ts = LocalDateTime.of(2020, 11, 19, 10, 16)
                    .toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
            Map<TopicPartition, Long> timeToSearch = consumer.partitionsFor(topic).stream().map(info ->
                    new TopicPartition(topic, info.partition())).collect(Collectors.toMap(Function.identity(), tp -> ts));
            for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : consumer.offsetsForTimes(timeToSearch).entrySet()) {
                consumer.seek(entry.getKey(), entry.getValue().offset());
            }
            // Duration 实现
            Map<TopicPartition, Long> timeToSearchs = consumer.partitionsFor(topic).stream().map(info ->
                    new TopicPartition(topic, info.partition())).collect(Collectors.toMap(Function.identity(), tp ->
                    System.currentTimeMillis() - 30 * 1000 * 60L));
            for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : consumer.offsetsForTimes(timeToSearchs).entrySet()) {
                consumer.seek(entry.getKey(), entry.getValue().offset());
            }
        }
    }
}
