package transformation;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Locale;
import java.util.Random;


public class TransformationApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();


//        map(environment);
//        flatMap(environment);

//        KeyBy(environment);
//        reduce(environment);
//        environment.setParallelism(2);
//        RichMap(environment);
        CoMap(environment);
        environment.execute("TransformationApp");

    }

    public static void StudentSource(StreamExecutionEnvironment env) {
        DataStreamSource<Student> source = env.addSource(new StudentSource());
        System.out.println(source.getParallelism());
        source.print();
    }


    public static void CoMap(StreamExecutionEnvironment env) {
        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 9527);
        SingleOutputStreamOperator<Integer> stream2 = env.socketTextStream("localhost", 9528).map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return Integer.parseInt(value);
            }
        });

        ConnectedStreams<String, Integer> connect = stream1.connect(stream2);
        connect.map(new CoMapFunction<String, Integer, String>() {
            @Override
            public String map1(String value) throws Exception {
                return value.toUpperCase();
            }

            @Override
            public String map2(Integer value) throws Exception {
                return value * 10 + "";
            }
        }).print();

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

    public static void RichMap(StreamExecutionEnvironment env) {

        DataStreamSource<String> source = env.readTextFile("datas/access.log");

        source.setParallelism(1).map(new MPFuncation()).print();
    }

    public static void defaultSource(StreamExecutionEnvironment env) {
        DataStreamSource<Access> source = env.addSource(new MPFuncation.AccessSource());
        System.out.println(source.getParallelism());
        source.print();
    }
}

class MPFuncation extends RichMapFunction<String, Access> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        System.out.println("~~~~open ~~~~");
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public RuntimeContext getRuntimeContext() {
        return super.getRuntimeContext();
    }

    @Override
    public Access map(String lines) throws Exception {

        System.out.println("~~~~~map ~~~");
        String[] splits = lines.split(",");
        long time = Long.parseLong(splits[0]);
        String domain = splits[1];
        double traffic = Double.parseDouble(splits[2]);

        return new Access(time, domain, traffic);
    }


    static class AccessSource implements SourceFunction<Access> {

        String[] domain = {"a.com", "b.com", "c.com", "d.com", "e.com"};

        boolean running = true;
        Random random = new Random();

        @Override
        public void run(SourceContext<Access> ctx) throws Exception {
            while (running) {
                for (int i = 0; i < 10; i++) {
                    Access access = new Access();
                    access.setTime(1234556L);
                    access.setDomain(domain[random.nextInt(domain.length)]);
                    access.setTraffic(random.nextDouble() + 1000);
                    ctx.collect(access);
                }
                Thread.sleep(5000);
            }

        }

        @Override
        public void cancel() {
            running = false;
        }
    }


}
