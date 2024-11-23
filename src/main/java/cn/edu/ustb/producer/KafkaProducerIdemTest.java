package cn.edu.ustb.kafkaModels.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.concurrent.Future;

/**
 * 演示生产数据时的幂等性操作
 */
public class KafkaProducerIdemTest {
    public static void main(String[] args) {
        HashMap<String, Object> configMap = new HashMap<>();
        configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configMap.put(ProducerConfig.ACKS_CONFIG, "all");
        configMap.put(ProducerConfig.RETRIES_CONFIG, 5);
        configMap.put(ProducerConfig.BATCH_SIZE_CONFIG, 5);
        configMap.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
        configMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // 启用幂等性操作，保证数据的不重复并且数据不乱序
        configMap.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        KafkaProducer<String, String> producer = new KafkaProducer<>(configMap);

        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("topic_1", "key" + i);
            //TODO 通过生产者对象将数据发送到Kafka
            //      异步发送数据，不用等待应答
            Future<RecordMetadata> send = producer.send(record, new Callback() {
                /**
                 * 当完成接收的时候，会返回当前的数据状态
                 * @param recordMetadata 元数据
                 * @param e 如果异常值为空，说明正常接收；如果异常值不为空，说明发送有问题
                 */
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    System.out.println("数据发送成功！" + record);
                }
            });
        }

        producer.close();
    }
}
