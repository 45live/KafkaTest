package cn.edu.ustb.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.concurrent.Future;

public class KafkaProducerIdemTest {
    public static void main(String[] args) {
        // TODO 创建配置对象
        HashMap<String, Object> configMap = new HashMap<>();
        // 指定配置参数
        configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "Hadoop130");
        configMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configMap.put(ProducerConfig.ACKS_CONFIG, "-1");
        configMap.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configMap.put(ProducerConfig.RETRIES_CONFIG, 5);
        configMap.put(ProducerConfig.BATCH_SIZE_CONFIG, 5);
        configMap.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);

        KafkaProducer<String, String> producer = new KafkaProducer<>(configMap);

        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("test_1", "key" + i, "value" + i);

            Future<RecordMetadata> send = producer.send(record);
        }

        producer.close();
    }
}
