package cn.baise;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformReduceTest {
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


        result.print("------> ");

        env.execute("TransformReduceTest");
    }
}
