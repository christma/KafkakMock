package flink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import entry.SinkInfo;
import entry.SomeInfo;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import utils.Constants;
import utils.FlinkKafkaConsumerUtils;
import utils.MySQLUtils;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;


/**
 * 1、超过延迟过大的问题，测输出流
 * 2、statebackend  存储位置
 * 3、指标问题
 * 4、维表问题
 * 5、
 *
 */
public class SinkMySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        TableEnvironment.getTableEnvironment(env);
        Properties properties = FlinkKafkaConsumerUtils.getConsumerProperties(Constants.BROKERS, Constants.TOPIC, "my_mock");

        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer<String>(Constants.TOPIC, new SimpleStringSchema(), properties));

//        source.print();
        source.flatMap(new FlatMapFunction<String, SomeInfo>() {
                    @Override
                    public void flatMap(String value, Collector<SomeInfo> out) throws Exception {
                        String body = JSON.parseObject(value).getString("body");
                        JSONObject body1 = JSON.parseObject(body);
                        Long tms = body1.getLong("time");
                        Timestamp time = body1.getTimestamp("time");//2022-02-17 23:28:04.629
                        String orgId = body1.getString("orgId");
                        JSONArray vars = body1.getJSONArray("vars");
                        SomeInfo someInfo = new SomeInfo();
                        for (int i = 0; i < vars.size(); i++) {
                            String tagCode = vars.getJSONObject(i).getString("tagCode");
                            String maskCode = vars.getJSONObject(i).getString("maskCode");
                            Integer values = vars.getJSONObject(i).getInteger("value");
                            Integer quality = vars.getJSONObject(i).getInteger("quality");
                            String s = time.toString();
                            int year = Integer.parseInt(s.substring(0, 4));
                            int month = Integer.parseInt(s.substring(5, 7));
                            int tmsDay = Integer.parseInt(s.substring(8, 10));
                            int hours = Integer.parseInt(s.substring(11, 13));
                            int tmsMinutes = Integer.parseInt(s.substring(14, 16));
                            int ceil = (int) Math.ceil(time.getMinutes() / 1);
                            someInfo.set_year(year);
                            someInfo.set_month(month);
                            someInfo.set_day(tmsDay);
                            someInfo.set_hour(hours);
                            someInfo.set_minute(tmsMinutes);
                            someInfo.set_part(ceil);
                            someInfo.setTimestamp(tms);
                            someInfo.setOrgId(orgId);
                            someInfo.setMaskCode(maskCode);
                            someInfo.setTagCode(tagCode);
                            someInfo.setValue(values);
                            someInfo.setQuality(quality);
                            someInfo.setKey(year + "_" + month + "_" + tmsDay + "_" + hours + "_" + ceil);

                            out.collect(someInfo);
                        }
                    }
                }).filter(x -> x.getQuality() == 192).assignTimestampsAndWatermarks(new EventTimeExtract())
                .keyBy(new KeySelector<SomeInfo, String>() {
                    @Override
                    public String getKey(SomeInfo value) throws Exception {
                        return value.getKey();
                    }
                }).process(new KeyedProcessFunc())
                .addSink(new MySQLSink())
        ;

        env.execute("SinkMySQL");


    }

    private static class KeyedProcessFunc extends KeyedProcessFunction<String, SomeInfo, SinkInfo> {

        private ListState<SomeInfo> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext()
                    .getListState(new ListStateDescriptor<>(
                            "myListState",
                            SomeInfo.class
                    ));
        }

        @Override
        public void processElement(SomeInfo value, KeyedProcessFunction<String, SomeInfo, SinkInfo>.Context ctx, Collector<SinkInfo> out) throws Exception {
            Iterable<SomeInfo> elements = state.get();
            if (elements == null) {
                state.addAll(Collections.emptyList());
            }
            state.add(value);
            ArrayList<SomeInfo> allElement = Lists.newArrayList(state.get().iterator());

            if (allElement.size() == 10) {

                SinkInfo sinkInfo = new SinkInfo();
                double sum = 0;
                double count = 0;
                double maxCount = Double.MIN_VALUE;
                double minCount = Double.MAX_VALUE;
                String key = "";
                for (SomeInfo si: allElement) {
                    count++;
                    sum += si.getValue();
                    maxCount = Math.max(maxCount, si.getValue());
                    minCount = Math.min(minCount, si.getValue());
                    key = si.getKey();
                }
                // 平均值
                double avgValue = sum / count;
                sinkInfo.setAvgValue(avgValue);
                sinkInfo.setMaxValue(maxCount);
                sinkInfo.setMinVlue(minCount);
                sinkInfo.setKey(key);
                out.collect(sinkInfo);
                state.clear();
            }
        }
    }

    private static class EventTimeExtract implements AssignerWithPeriodicWatermarks<SomeInfo> {

        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");
        private long currentMaxEventTime = 0L;
        private long maxOufOfOrderness = 10000;//最大乱序时间

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxEventTime - maxOufOfOrderness);
        }

        @Override
        public long extractTimestamp(SomeInfo element, long recordTimestamp) {
            Long currentElementTime = element.getTimestamp();
            currentMaxEventTime = Math.max(currentMaxEventTime, currentElementTime);
            return currentElementTime;
        }
    }

    private static class MySQLSink extends RichSinkFunction<SinkInfo> {

        Connection connection;
        PreparedStatement insertPstm;
        PreparedStatement updatePstm;
//
//        kk varchar(10) primary key ,
//        b_time long,
//        b_avg double,
//        b_first_value double,
//        b_last_value double,
//        b_max_value double,
//        b_min_value double

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = MySQLUtils.getConnection();
            insertPstm = connection.prepareStatement("insert into test(kk,b_time,b_avg,b_first_value,b_last_value,b_max_value,b_min_value)values(?,?,?,?,?,?,?)");
            updatePstm = connection.prepareStatement("update test set b_time = ?,b_avg = ?,b_first_value = ?,b_last_value= ?,b_max_value=?,b_min_value=? where kk = ?");
        }

        //insert into access(domain,traffic)values(a.com,10000)

        @Override
        public void close() throws Exception {
            MySQLUtils.close(connection, insertPstm);
            MySQLUtils.close(connection, updatePstm);
        }

        @Override
        public void invoke(SinkInfo value, Context context) throws Exception {
            System.out.println("update"+value);
            updatePstm.setLong(1, value.getTime());
            updatePstm.setDouble(2, value.getAvgValue());
            updatePstm.setDouble(3, value.getFirstValue());
            updatePstm.setDouble(4, value.getLastValue());
            updatePstm.setDouble(5, value.getMaxValue());
            updatePstm.setDouble(6, value.getMinVlue());
            updatePstm.setString(7,value.getKey());
            updatePstm.execute();
            if (updatePstm.getUpdateCount() == 0) {
                System.out.println("insert"+value);

                insertPstm.setString(1, value.getKey());
                insertPstm.setLong(2, value.getTime());
                insertPstm.setDouble(3, value.getAvgValue());
                insertPstm.setDouble(4, value.getFirstValue());
                insertPstm.setDouble(5, value.getLastValue());
                insertPstm.setDouble(6, value.getMaxValue());
                insertPstm.setDouble(7, value.getMinVlue());
                insertPstm.execute();
            }
        }
    }
}
