package mock;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleSerializers;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import utils.ETLUtils;

import java.util.Properties;
import java.util.concurrent.Future;

public class IdempotentProducerExample {
    public static void main(String[] args) {
        // Kafka 生产者配置
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Kafka 集群的地址
//        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, String);
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // 启用幂等性
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // 所有副本都需要确认
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE); // 最大重试次数
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // 在单连接中允许的未确认请求数

        // 创建 Kafka 生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        try {
            // 发送消息
            ETLUtils.sendKafka(producer,"mock-risk","value");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 关闭生产者
            producer.close();
        }
    }
}
