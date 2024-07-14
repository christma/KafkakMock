package utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class KafkaProducerUtils {

    static Producer<String, String> producer;

    public static void init() {
        Properties props = new Properties();
        //此处配置的是kafka的端口
//        props.put("broker.list", Constants.BROKERS);
        props.put("bootstrap.servers", Constants.BROKERS);
//        props.put("topic", Constants.TOPIC);

        //配置value的序列化类
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //配置key的序列化类
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("producer.type", "async");

        props.put("request.required.acks", "-1");
        producer = new KafkaProducer<>(props);
    }

    public static Producer getProducer() {
        if (producer == null) {
            init();
        }
        return producer;
    }

}
