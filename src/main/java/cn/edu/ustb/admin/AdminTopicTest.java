package cn.edu.ustb.admin;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class AdminTopicTest {
    public static void main(String[] args) {
        //创建配置集合，告诉管理员如何去连接Kafka集群
        HashMap<String, Object> confMap = new HashMap<>();
        confMap.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "Hadoop130:9092,Hadoop131:9092,Hadoop132:9092");

        //TODO 创建管理员对象
        final Admin admin = Admin.create(confMap);
        //第一个参数为主题的名称（字母，数字，点，下划线，中横线），第二个参数为分区数，第三个参数为副本数
        final NewTopic topic1 = new NewTopic("test_1", 1, (short) 1);
        final NewTopic topic2 = new NewTopic("test_2", 2, (short) 2);

        HashMap<Integer, List<Integer>> map = new HashMap<>();
        map.put(0, Arrays.asList(3, 1));
        map.put(1, Arrays.asList(2, 3));
        map.put(2, Arrays.asList(1, 2));
        NewTopic topic3 = new NewTopic("test_3", map);

        //TODO 创建主题
        final CreateTopicsResult result = admin.createTopics(Arrays.asList(topic1, topic2, topic3));

        // In - Sync -Replicas 同步副本列表（ISR）
        System.out.println(result);

        //TODO 关闭连接
        admin.close();
    }
}
