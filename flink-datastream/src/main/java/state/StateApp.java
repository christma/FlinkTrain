package state;

import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class StateApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        valueState(env);
        env.execute("StateApp");
    }

    private static void valueState(StreamExecutionEnvironment env) {

        List<Tuple2<Long, Long>> list = new ArrayList<>();


        list.add(Tuple2.of(1L, 2L));
        list.add(Tuple2.of(1L, 3L));
        list.add(Tuple2.of(2L, 4L));
        list.add(Tuple2.of(2L, 4L));
        list.add(Tuple2.of(3L, 2L));
        list.add(Tuple2.of(4L, 1L));
        list.add(Tuple2.of(3L, 2L));
        list.add(Tuple2.of(4L, 1L));

        env.fromCollection(list)
                .keyBy(value -> value.f0)
//                .flatMap(new CountWindowsAvg())
                .flatMap(new AvgWithMapState())
                .print();


    }


    private static class AvgWithMapState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {


        private transient MapState<String, Long> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {

            MapStateDescriptor<String, Long> descriptor = new MapStateDescriptor<>("avg", String.class, Long.class);

            mapState = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Double>> out) throws Exception {

            mapState.put(UUID.randomUUID().toString(), value.f1);
            ArrayList<Long> elements = Lists.newArrayList(mapState.values());

            if (elements.size() >= 2) {
                long count = 0L;
                long sum = 0L;
                for (Long element : elements) {
                    count++;
                    sum += element;
                }
                double avg = (double) sum / count;
                out.collect(Tuple2.of(value.f0, avg));
                mapState.clear();
            }

        }
    }


    private static class CountWindowsAvg extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

        private transient ValueState<Tuple2<Long, Long>> sum;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Tuple2<Long, Long>> descriptor = new ValueStateDescriptor<>(
                    "average", // the state name
                    TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                    }), // type information
                    Tuple2.of(0L, 0L));

            sum = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {

            Tuple2<Long, Long> currentSum = sum.value();

            currentSum.f0 += 1;
            currentSum.f1 += value.f1;

            sum.update(currentSum);

            if (currentSum.f0 >= 2) {
                out.collect(new Tuple2<>(value.f0, currentSum.f1 / currentSum.f0));
                sum.clear();
            }
        }
    }
}
