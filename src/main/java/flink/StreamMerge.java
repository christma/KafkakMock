package flink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.util.Collector;
import utils.Constants;
import utils.FlinkKafkaConsumerUtils;

import java.time.Duration;
import java.util.Properties;

public class StreamMerge {
    private static String BROKERS = Constants.BROKERS;
    private static String TOPIC = Constants.TOPIC;


    public static void main(String[] args) throws Exception {
        String GROUP_ID = "flink_merge";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000);
        env.setParallelism(2);

        Properties properties = FlinkKafkaConsumerUtils.getConsumerProperties(BROKERS, TOPIC, GROUP_ID);

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(TOPIC, new SimpleStringSchema(), properties);
        FlinkKafkaConsumerBase<String> source = kafkaConsumer.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)));
        DataStreamSource<String> streamSource = env.addSource(source);
        SingleOutputStreamOperator<Tuple2<String, String>> streamOperator = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, String>> out) throws Exception {
                        JSONObject object = JSON.parseObject(value);
                        String id = object.getString("id");
                        out.collect(Tuple2.of(id, value));
                    }
                }).keyBy(key -> key.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new MergeProcessWindowFunction());
        streamOperator.print();

//
//        Properties producerProperties = FlinkKafkaProducerUtils.getProducerProperties("localhost");
//        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(Constants.SINK_TOPIC, new SimpleStringSchema(), producerProperties);

//        DataStreamSink<Tuple2<String, String>> sink = streamOperator.addSink();


        env.execute("StreamMerge");
    }


    private static class MergeProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, String>, Tuple2<String, String>, String, TimeWindow> {


        @Override
        public void process(String s, ProcessWindowFunction<Tuple2<String, String>, Tuple2<String, String>, String, TimeWindow>.Context context, Iterable<Tuple2<String, String>> elements, Collector<Tuple2<String, String>> out) throws Exception {
//
//            System.out.println(context.currentProcessingTime());
//            System.out.println(context.currentWatermark());

            JSONArray jsonArray = new JSONArray();
            for (Tuple2<String, String> ele: elements) {
                jsonArray.add(ele.f1);
            }
            out.collect(Tuple2.of(s, jsonArray.toString()));
        }


    }
}
