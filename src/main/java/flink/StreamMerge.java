package flink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.fastjson.serializer.JSONSerializer;
import entry.OrderInfo;
import jdk.jshell.execution.StreamingExecutionControl;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import utils.Constants;
import utils.FlinkKafkaConsumerUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

public class StreamMerge {
    private static String BROKERS = Constants.BROKERS;
    private static String TOPIC = Constants.TOPIC;


    public static void main(String[] args) throws Exception {
        String GROUP_ID = "flink_merge";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000);
        env.setParallelism(2);
//        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Properties properties = FlinkKafkaConsumerUtils.getConsumerProperties(BROKERS, TOPIC, GROUP_ID);

        DataStreamSource<String> source
                = env.addSource(new FlinkKafkaConsumer<>(TOPIC, new SimpleStringSchema(), properties));


//        KafkaSource.builder()
//                .setBootstrapServers(BROKERS)
//                .setTopics("input-topic")
//                .setGroupId("my-group")
//                .setStartingOffsets(OffsetsInitializer.earliest())
//                .setValueOnlyDeserializer(new SimpleStringSchema())
//                .build();

        source.flatMap(new FlatMapFunction<String, OrderInfo>() {
                    @Override
                    public void flatMap(String value, Collector<OrderInfo> out) throws Exception {
                        OrderInfo orderInfo = JSON.parseObject(value, new TypeReference<OrderInfo>() {
                        });

                        out.collect(orderInfo);
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)))
                .keyBy(line -> line.getId());


        env.execute("StreamMerge");
    }

}
