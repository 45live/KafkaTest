package cn.edu.ustb.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class KafkaTopicCleaner {
    private static final Logger log = LoggerFactory.getLogger(cn.edu.ustb.kafkaModels.consumer.KafkaConsumerAutoOffsetTest.class);

    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092"; // Kafka broker地址
        String topicName = "topic_1"; // 要清空的topic的名称

        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        try (AdminClient adminClient = AdminClient.create(properties)) {

            // 删除topic
            DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Collections.singletonList(topicName));
            deleteTopicsResult.all().get(); // 确保删除请求已发送

            // 等待主题完全删除
            waitForTopicDeletion(adminClient, topicName);

            // 创建新的同名topic
            NewTopic newTopic = new NewTopic(topicName, 1, (short) 2); // 根据需要设置分区数和副本因子
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();

            System.out.println("Topic " + topicName + " has been cleared and recreated.");
        } catch (InterruptedException | ExecutionException e) {
            log.info("Error while creating topic {}", topicName, e);
        }
    }

    private static void waitForTopicDeletion(AdminClient adminClient, String topicName) throws ExecutionException, InterruptedException {
        while (true) {
            ListTopicsResult listTopicsResult = adminClient.listTopics();
            if (!listTopicsResult.names().get().contains(topicName)) {
                break; // 主题已删除
            }
            TimeUnit.SECONDS.sleep(1); // 等待一段时间再检查
        }
    }
}