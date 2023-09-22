package kafka;

/**
 * 该代码使用了默认配置，并使用group.id消费组来读取topic数据，当前存在这几种情况：
 * 1.消费者数量 == partitions数量 每个消费组能读取到一个固定的partition的数据
 * 2.消费者数量 > partitions数量 比如2个消费者1个partition，只有一个消费者能读取到数据
 * 3.消费者数量 < partitions数量 比如2个消费者3个partition, 每个消费者能读取到一个固定的partition的数据，第三个消费根据消费策略来确定谁读取。默认是第一个消费者读取
 * @Author jinlian
 * @create 2023/9/21 15:27
 */

import kafka.config.KafkaConfig;
import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.*;


public class KafkaExample {


    public static void main(String[] args) {
        // 配置消费者
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group2");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        try {
            // 订阅主题
            consumer.subscribe(Collections.singletonList(KafkaConfig.TOPIC_NAME));

            while (true) {
                // 拉取消息
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    // 处理消息
                    System.out.println("Received message: " + record.value());
                }

                // 提交偏移量
                consumer.commitSync();
            }
        } finally {
            consumer.close();
        }
    }
}
