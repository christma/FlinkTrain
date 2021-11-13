import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import scala.Int;

import java.util.concurrent.ExecutionException;

public class BatchWC {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        DataSource<String> source = env.readTextFile("/Users/apple/IdeaProjects/FlinkTrain/datas/wc.data");

        source.flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String lines, Collector<String> out) throws Exception {
                        String[] words = lines.split(",");
                        for (String word : words) {
                            out.collect(word);
                        }
                    }
                }).filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String s) throws Exception {
                        return StringUtils.isNotEmpty(s);
                    }
                }).map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        return new Tuple2<>(s, 1);
                    }
                })

                .groupBy(0).sum(1).print();

    }
}
