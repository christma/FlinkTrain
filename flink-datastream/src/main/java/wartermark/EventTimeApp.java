package wartermark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.stream.Stream;

public class EventTimeApp {


    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        MyWaterMarker(env);
        env.execute("EventTimeApp");
    }

    private static void MyWaterMarker(StreamExecutionEnvironment env) {

        SingleOutputStreamOperator<String> lines = env.socketTextStream("localhost", 9527)
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
                            @Override
                            public long extractTimestamp(String element) {
                                return Long.parseLong(element.split(",")[0]);
                            }
                        }
                );

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] splits = value.split(",");
                return Tuple2.of(splits[1].trim(), Integer.parseInt(splits[2].trim()));
            }
        });

        map.keyBy(x -> x.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum(1)
                .print();


    }


}
