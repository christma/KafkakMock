package test;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class TimeWindowWordCount {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> dataStream = env.addSource(new TestSouce());

        dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>>
                            out) throws Exception {
                        String[] fields = line.split(",");
                        for (String word: fields) {
                            out.collect(new Tuple2<>(word, 1));
                        }
                    }
                }).keyBy(x -> x.f0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new SumProcessWindowFunction()).print();
        env.execute("TimeWindowWordCount");
    }

    public static class TestSouce implements SourceFunction<String> {
        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

        @Override
        public void run(SourceContext<String> ctx) throws Exception {

// ??????????????? 10 ????????????????????????????????????
            String currTime = String.valueOf(System.currentTimeMillis());
            while (Integer.valueOf(currTime.substring(currTime.length() - 4)) > 100) {
                currTime = String.valueOf(System.currentTimeMillis());
                continue;
            }

            System.out.println("???????????????????????????:" + dateFormat.format(System.currentTimeMillis()));
// ??? 13 ?????????????????????
            TimeUnit.SECONDS.sleep(13);
            ctx.collect("hadoop," + System.currentTimeMillis()); // ?????????????????????????????????????????????????????????????????????
            String event = "hadoop," + System.currentTimeMillis(); // ??? 16 ?????????????????????
            TimeUnit.SECONDS.sleep(3);
            ctx.collect("hadoop," + System.currentTimeMillis()); // ??? 19 ??????????????????
            TimeUnit.SECONDS.sleep(3);
            ctx.collect(event);

        }

        @Override
        public void cancel() {
        }
    }

    private static class SumProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> {

        @Override
        public void process(String s, ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>.Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
            int sum = 0;
            for (Tuple2<String, Integer> ele: elements) {
                sum += 1;
            }
// ???????????????????????????
            out.collect(Tuple2.of(s, sum));
        }
    }
}
