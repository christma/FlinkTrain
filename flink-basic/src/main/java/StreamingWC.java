import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StreamingWC {

    private static final Logger logger = LoggerFactory.getLogger(StreamingWC.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();


        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = source.flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String lines, Collector<String> out) throws Exception {
                        String[] words = lines.split(",");
                        for (String word : words) {
                            out.collect(word.toLowerCase().trim());
                        }
                    }
                }).filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String s) throws Exception {
                        return StringUtils.isNotEmpty(s);
                    }
                }).map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return new Tuple2<>(value, 1);
                    }
                }).keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
                    @Override
                    public Object getKey(Tuple2<String, Integer> key) throws Exception {
                        return key.f0;
                    }
                })
                .sum(1);

        sum.print();


        env.execute("StreamingWC");

    }
}
