package cn.edu.ustb.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.ArrayList;
import java.util.HashMap;

public class KafkaSparkStreamingTest {
    public static void main(String[] args) {
        // 初始化Spark的环境配置
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("KafkaSparkStreamingTest");
        sparkConf.set("spark.streaming.materializer", "kafka");

        // 创建JavaSparkContext环境
        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, new Duration(3 * 1000));

        // 创建Kafka的配置参数
        HashMap<String, Object> configMap = new HashMap<>();
        configMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        configMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configMap.put(ConsumerConfig.GROUP_ID_CONFIG, "djx");
        configMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(configMap);

        // 创建需要消费的主题
        ArrayList<String> strings = new ArrayList<>();
        strings.add("topic_1");

        JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils.createDirectStream(jsc, LocationStrategies.PreferBrokers(), ConsumerStrategies.Subscribe(strings, configMap));
        directStream.map(new Function<ConsumerRecord<String, String>, String>() {
                    @Override
                    public String call(ConsumerRecord<String, String> stringStringConsumerRecord) throws Exception {
                        return stringStringConsumerRecord.value();
                    }
                })
                .print();

        jsc.start();
        try {
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
