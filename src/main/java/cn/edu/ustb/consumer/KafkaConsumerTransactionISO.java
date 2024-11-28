package cn.edu.ustb.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class KafkaConsumerTransactionISO {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerTransactionISO.class);

    public static void main(String[] args) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "Hadoop131:9092");
        configMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configMap.put(ConsumerConfig.GROUP_ID_CONFIG, "root");
        configMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configMap.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configMap);

        //TODO 订阅主题
        consumer.subscribe(Collections.singletonList("topic_1"));

        //TODO 从Kafka主题中获取数据
        //      消费者从Kafka中拉取数据
        try {
            while (true) {
                ConsumerRecords<String, String> datas = consumer.poll(Duration.ofMillis(100L));
                for (ConsumerRecord<String, String> data : datas) {
                    System.out.println(data);
                }
            }
        } catch (Exception e) {
            log.info("出现异常，异常信息为：{}", e.getMessage());
        } finally {
            consumer.close();
        }
    }
}
