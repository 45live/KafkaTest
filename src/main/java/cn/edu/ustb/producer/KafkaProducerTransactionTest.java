package cn.edu.ustb.kafkaModels.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class KafkaProducerTransactionTest {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducerTransactionTest.class);

    public static void main(String[] args) {
        HashMap<String, Object> configMap = new HashMap<>();
        configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configMap.put(ProducerConfig.ACKS_CONFIG, "all");
        configMap.put(ProducerConfig.RETRIES_CONFIG, 5);
        configMap.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        configMap.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        configMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configMap.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configMap.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-transactional-id");

        KafkaProducer<String, String> producer = new KafkaProducer<>(configMap);

        // 初始化事务操作
        producer.initTransactions();

        try {
            // 开启事务操作
            producer.beginTransaction();
            log.info("开启Kafka的事务操作！");

            for (int i = 0; i < 10; i++) {
                producer.send(new ProducerRecord<>("topic_1", "key" + i, "value" + i), (recordMetadata, e) -> System.out.println("数据发送成功！元数据信息为：" + recordMetadata));
            }
            producer.commitTransaction();
        } catch (Exception e) {
            // 如果出错，回滚事务
            producer.abortTransaction();
        } finally {
            producer.close();
        }
    }
}
