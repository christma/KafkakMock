package mock;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class ConsumerData {


    private static Properties configByKafkaServer() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "");
        props.setProperty("group.id", "test_bll_group");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.addSource(
                new FlinkKafkaConsumer<>("testIn", new SimpleStringSchema(), configByKafkaServer()));

        System.out.println(source);
        env.execute("ConsumerData");

    }
}
