package cn.baise;

import com.mysql.jdbc.TimeUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;


public class SinkToFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> source = env.addSource(new ParallelCustomSource()).setParallelism(1);

        SingleOutputStreamOperator<Tuple2<Integer, Long>> reduce = source.map(new MapFunction<Integer, Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> map(Integer integer) throws Exception {
                        return Tuple2.of(integer, 1L);
                    }
                }).keyBy(data -> data.f0)
                .reduce(new ReduceFunction<Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> reduce(Tuple2<Integer, Long> v1, Tuple2<Integer, Long> v2) throws Exception {
                        return Tuple2.of(v1.f0, v1.f1 + v2.f1);
                    }
                });


        SingleOutputStreamOperator<Tuple2<Integer, Long>> result = reduce.keyBy(data -> "key").reduce(new ReduceFunction<Tuple2<Integer, Long>>() {
            @Override
            public Tuple2<Integer, Long> reduce(Tuple2<Integer, Long> v1, Tuple2<Integer, Long> v2) throws Exception {
                return v1.f1 > v2.f1 ? v1 : v2;
            }
        });


        StreamingFileSink<String> streamingFileSink = StreamingFileSink.<String>forRowFormat(new Path("/Users/apple/IdeaProjects/FlinkTrain/datas/output"), new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withMaxPartSize(1024 * 1024)
                                .withRolloverInterval(TimeUnit.MINUTES.toMinutes(10))
                                .withInactivityInterval(TimeUnit.MINUTES.toMinutes(5)).build())
                .build();


        result.print();
        result.map(data -> data.toString()).addSink(streamingFileSink);


        env.execute("SinkToFile");


    }
}
