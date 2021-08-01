package com.yw.middleware.example.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author yangwei
 */
public class MyPartitioner implements Partitioner {
    /**
     * 通过这个方法来实现消息要去哪一个分区中
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // 获取分区数
        int partitions = cluster.partitionsForTopic(topic).size();
        // key.hashCode()可能会出现负数 -1 -2 0 1 2
        // Math.abs 取绝对值
        return Math.abs(key.hashCode() % partitions);
    }
    @Override
    public void close() {

    }
    @Override
    public void configure(Map<String, ?> configs) {
    }
}
