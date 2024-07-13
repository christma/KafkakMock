package flink;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;
import utils.FlinkKafkaConsumerUtils;

import java.util.Properties;

public class SinkRedisApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        Properties properties = FlinkKafkaConsumerUtils.getConsumerProperties("localhost:9092", "mock", "myGroup");
        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer<>("mock", new SimpleStringSchema(), properties));


        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost")
                .setPort(6379)
                .build();

        source.flatMap(new FlatMapFunction<String, Tuple2<Integer, String>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<Integer, String>> out) throws Exception {
                System.out.println(value);
                JSONObject object = JSON.parseObject(value);
                Integer id = object.getInteger("id");
                out.collect(Tuple2.of(id, value));
            }
        }).addSink(new RedisSink<>(conf, new SinkRedisMapper()));


        env.execute("SinkRedisApp");


    }

    private static class SinkRedisMapper implements RedisMapper<Tuple2<Integer, String>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "mock");
        }

        @Override
        public String getKeyFromData(Tuple2<Integer, String> data) {
            return data.f0.toString();
        }

        @Override
        public String getValueFromData(Tuple2<Integer, String> data) {
            return data.f1;
        }
    }
}
