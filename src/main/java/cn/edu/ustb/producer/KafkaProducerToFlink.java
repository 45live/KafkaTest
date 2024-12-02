package cn.edu.ustb.kafkaModels.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Random;

public class KafkaProducerToFlink {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducerToFlink.class);
    private static final String[] WORDS = {
            "apple", "banana", "orange", "grape", "kiwi",
            "mango", "pear", "peach", "strawberry", "blueberry"
    };

    public static void main(String[] args) {
        // 设置生产数据的相关配置
        HashMap<String, Object> configMap = new HashMap<>();
        configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configMap.put(ProducerConfig.ACKS_CONFIG, "all");
        configMap.put(ProducerConfig.RETRIES_CONFIG, 5);
        configMap.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        configMap.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        configMap.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        configMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // 创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(configMap);

        Random random = new Random();

        // 生产随机单词的数据
        for (int i = 0; i < 1000; i++) {
            // 随机选择一个单词
            String randomWord = WORDS[random.nextInt(WORDS.length)];
            // 发送数据到Kafka
            producer.send(new ProducerRecord<>("topic_1", "key" + i, randomWord), (RecordMetadata recordMetadata, Exception e) -> {
                if (e != null) {
                    log.error(e.getMessage(), e);
                } else {
                    System.out.println("数据发送成功！" + recordMetadata);
                }
            });

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        producer.close();
    }
}
