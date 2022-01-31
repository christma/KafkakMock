package flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import utils.Constants;
import utils.FlinkKafkaConsumerUtils;
import utils.FlinkKafkaProducerUtils;

import java.util.Properties;

public class SinkKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = FlinkKafkaConsumerUtils.getConsumerProperties(Constants.BROKERS, Constants.TOPIC, "formock");

        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer<String>(Constants.TOPIC, new SimpleStringSchema(), properties));

        source.print();
        Properties saveMock = FlinkKafkaProducerUtils.getProducerProperties(Constants.BROKERS);

        source.addSink(new FlinkKafkaProducer<String>(Constants.SINK_TOPIC, new SimpleStringSchema(), saveMock))
                .name("saveKafka");

        env.execute("SinkKafka");

    }
}
