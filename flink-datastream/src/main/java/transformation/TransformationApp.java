package transformation;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Int;

public class TransformationApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();


//        map(environment);
//        flatMap(environment);

//        KeyBy(environment);
        reduce(environment);
        environment.execute("TransformationApp");

    }


    public static void map(StreamExecutionEnvironment env) {

        DataStreamSource<String> source = env.readTextFile("datas/access.log");

        SingleOutputStreamOperator<Access> mapStream = source.map(new MapFunction<String, Access>() {
            @Override
            public Access map(String str) throws Exception {
                String[] splits = str.split(",");
                if (splits.length == 3) {
                    long time = Long.parseLong(splits[0].trim());
                    String domain = splits[1].trim();
                    Double traffic = Double.parseDouble(splits[2].trim());
                    return new Access(time, domain, traffic);
                }
                return null;
            }
        }).filter(new FilterFunction<Access>() {
            @Override
            public boolean filter(Access access) throws Exception {
                return !"".equals(access);

            }
        });

        mapStream.print();
    }

    public static void flatMap(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);

        source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String lines, Collector<String> collector) throws Exception {
                String[] splits = lines.split(",");
                for (String split : splits) {
                    collector.collect(split);
                }
            }
        }).print();
    }

    public static void KeyBy(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.readTextFile("datas/access.log");
        source.map(new MapFunction<String, Access>() {
                    @Override
                    public Access map(String str) throws Exception {
                        String[] splits = str.split(",");
                        long time = Long.parseLong(splits[0]);
                        String domain = splits[1];
                        double traffic = Double.parseDouble(splits[2]);

                        return new Access(time, domain, traffic);
                    }
                })//.keyBy("domain").sum("traffic").print();
                .keyBy(new KeySelector<Access, String>() {
                    @Override
                    public String getKey(Access access) throws Exception {
                        return access.getDomain();
                    }
                }).sum("traffic").print();
    }

    public static void reduce(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);

        source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String lines, Collector<Tuple2<String, Integer>> collector) throws Exception {

                        String[] splits = lines.split(",");
                        for (String word : splits) {
                            collector.collect(new Tuple2<>(word, 1));
                        }
                    }
                })//.keyBy(0).sum(1).print();
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> tuple2) throws Exception {
                        return tuple2.f0;
                    }
                }).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                        return Tuple2.of(t1.f0, t1.f1 + t2.f1);
                    }
                }).print();
    }

}