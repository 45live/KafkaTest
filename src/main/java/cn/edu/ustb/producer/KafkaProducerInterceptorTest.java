package cn.edu.ustb.producer;

import cn.edu.ustb.producer.function.ValueInterceptor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;

public class KafkaProducerInterceptorTest {
    public static void main(String[] args) {
        HashMap<String, Object> configMap = new HashMap<>();
        configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "Hadoop131:9092");
        configMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //TODO 自定义producer的Interceptor类
        configMap.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ValueInterceptor.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(configMap);

        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("test_1", "key" + i, "value" + i);
            producer.send(record);
        }

        producer.close();
    }
}
