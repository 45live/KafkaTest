package cn.edu.ustb.producer.function;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 自定义分区器
 * <p>
 * 1. 实现partitioner接口
 * </p>
 */
public class MyKafkaPartitioner implements Partitioner {

    /**
     * 根据key和value计算分区编号
     *
     * @param topic      The topic name
     * @param key        The key to function on (or null if no key)
     * @param keyBytes   The serialized key to function on( or null if no key)
     * @param value      The value to function on or null
     * @param valueBytes The serialized value to function on or null
     * @param cluster    The current cluster metadata
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
