package cn.edu.ustb.kafkaModels.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaConsumerAutoOffsetTest {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerAutoOffsetTest.class);

    public static void main(String[] args) {
        HashMap<String, Object> configMap = new HashMap<>();
        configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        configMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configMap.put(ConsumerConfig.GROUP_ID_CONFIG, "poll_data_from_topic_1");
        // TODO 重置偏移量
        // configMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configMap);
        String topic = "topic_1";
        consumer.subscribe(Collections.singletonList(topic));

        // TODO 如果想要在中间的位置进行读取，只需要获取集群信息，并重置所订阅的主题的偏移量即可
        //  获取集群信息
        AtomicBoolean flag = new AtomicBoolean(true);
        while (flag.get()) {
            consumer.poll(Duration.ofMillis(100));
            Set<TopicPartition> topicPartitions = consumer.assignment();
            if (topicPartitions != null && !topicPartitions.isEmpty()) {
                topicPartitions.forEach(topicPartition -> {
                    if (topic.equals(topicPartition.topic())) {
                        consumer.seek(topicPartition, 2);
                        flag.set(false);
                    }
                });
            }
        }

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(record -> System.out.println("Received Message：key = " + record.key() + ", value = " + record.value()));
            }
        } catch (Exception e) {
            log.info("出现异常，请尽快处理，异常信息为：{}", e.getMessage());
        } finally {
            consumer.close();
        }
    }
}
