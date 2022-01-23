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

        //request.required.acks
        //0, which means that the producer never waits for an acknowledgement from the broker (the same behavior as 0.7). This option provides the lowest latency but the weakest durability guarantees (some data will be lost when a server fails).
        //1, which means that the producer gets an acknowledgement after the leader replica has received the data. This option provides better durability as the client waits until the server acknowledges the request as successful (only messages that were written to the now-dead leader but not yet replicated will be lost).
        //-1, which means that the producer gets an acknowledgement after all in-sync replicas have received the data. This option provides the best durability, we guarantee that no messages will be lost as long as at least one in sync replica remains.
        props.put("request.required.acks", "-1");

        producer = new KafkaProducer<>(props);

    }

    private static void sendMessageCallBack() {
        ProducerRecord<String, String> record;
        while (true) {
            record = new ProducerRecord<>("mock", "CallBack");
            System.out.println(record);
            getProducer().send(record, new MyProducerCallBack());
            try {
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static Producer getProducer() {
        if (producer == null) {
            init();
        }
        return producer;
    }

    private static class MyProducerCallBack implements org.apache.kafka.clients.producer.Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (null != e) {
                e.printStackTrace();
                return;
            }
            System.out.println("时间戳，主题，分区，位移: " + recordMetadata.timestamp() + ", " + recordMetadata.topic() + "," + recordMetadata.partition() + " " + recordMetadata.offset());
        }
    }

    public static void main(String[] args) {
        ProducerRecord<String, String> record;
        for (int i = 0; i < 10; i++) {
            record = new ProducerRecord<>("mock", "------");
            System.out.println(record);

            getProducer().send(record);
        }

//        sendMessageCallBack();
    }
}
