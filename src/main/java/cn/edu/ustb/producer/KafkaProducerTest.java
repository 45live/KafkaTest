package cn.edu.ustb.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

public class KafkaProducerTest {
    public static void main(String[] args) {
        //TODO 创建生产者对象
        //      生产者对象需要设定泛型
        //      创建配置对象
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //对于生产的数据中的key和value进行序列化操作
        configMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(configMap);

        for (int i = 0; i < 100; i++) {
            //TODO 创建数据
            //      构建数据时，传递三个参数
            //      第一个参数表示主题的名称
            //      第二个参数表示数据的KEY
            //      第二个参数表示数据的VALUE
            //      采用循环多发送几条数据
            //      key会告诉我们value发送给哪一个broker
            ProducerRecord<String, String> record = new ProducerRecord<>("topic_1", "key" + i, "value" + i);
            System.out.println("数据发送成功！" + record);

            //TODO 通过生产者对象将数据发送到Kafka
            producer.send(record);
        }

        //TODO 关闭生产者对象
        producer.close();
    }
}
