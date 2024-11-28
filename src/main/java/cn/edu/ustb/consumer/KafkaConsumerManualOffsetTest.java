package cn.edu.ustb.kafkaModels.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;

public class KafkaConsumerManualOffsetTest {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerAutoOffsetTest.class);

    public static void main(String[] args) {
        HashMap<String, Object> configMap = new HashMap<>();
        configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        configMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configMap.put(ConsumerConfig.GROUP_ID_CONFIG, "poll_data_from_topic_1");
        configMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // TODO 关闭消费者自动保存偏移量的设置，开始进行手动提交
        configMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configMap);
        String topic = "topic_1";
        consumer.subscribe(Collections.singletonList(topic));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(record -> System.out.println("Received Message：key = " + record.key() + ", value = " + record.value()));

                // TODO 手动保存偏移量 - 此时指定为同步提交
                consumer.commitSync();
            }
        } catch (Exception e) {
            log.info("出现异常，请尽快处理，异常信息为：{}", e.getMessage());
        } finally {
            consumer.close();
        }
    }
}
