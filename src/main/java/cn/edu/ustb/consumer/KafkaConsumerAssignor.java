package cn.edu.ustb.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class KafkaConsumerAssignor {
    public static void main(String[] args) {

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "Hadoop131:9092");
        configMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configMap.put(ConsumerConfig.GROUP_ID_CONFIG, "root");

        // 指定组里的每一个消费者的memberId
        configMap.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "aaa");

        // 轮询分配策略
        configMap.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configMap);

        consumer.subscribe(Arrays.asList("topic_1", "test_1"));

        try {
            while (true) {
                ConsumerRecords<String, String> datas = consumer.poll(Duration.ofMillis(100L));
                for (ConsumerRecord<String, String> data : datas) {
                    System.out.println(data);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

            consumer.close();
        }
    }
}
