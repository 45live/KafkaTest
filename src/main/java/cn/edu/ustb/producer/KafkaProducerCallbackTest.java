package cn.edu.ustb.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaProducerCallbackTest {
    public static void main(String[] args) {
        HashMap<String, Object> configMap = new HashMap<>();
        configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "Hadoop131:9092");
        configMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(configMap);

        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("test_1", "key" + i, "value" + i);

            //TODO 通过生产者对象将数据发送到Kafka
            //      异步发送数据，我发我的，不用等待应答
            Future<RecordMetadata> send = producer.send(record, new Callback() {
                /**
                 * 当完成接收之后，会返回当前的数据处理状态
                 * @param metadata 元数据
                 * @param exception 如果异常值为空，说明正常接收；如果异常值不为空，说明发送有问题
                 */
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    System.out.println("数据发送成功！" + metadata);
                }
            });

            try {
                //TODO 当执行完应答操作之后，才会继续发送数据，否则会阻塞在这里
                send.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
            System.out.println("发送数据");
        }

        producer.close();
    }
}
