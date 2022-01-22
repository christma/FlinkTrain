package base;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordAndCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);
        source.flatMap(new SplitWordFunction())
                .keyBy(x->x.word)
                .sum(1)
                .print();

        env.execute("WordAndCount");
    }


    private static class SplitWordFunction implements FlatMapFunction<String, WordCount> {

        @Override
        public void flatMap(String value, Collector<WordCount> out) throws Exception {
            String[] splits = value.split(",");
            for (String split : splits) {
                out.collect(new WordCount(split, 1));
            }

        }
    }

    private static class WordCount {
        private String word;
        private Integer count;

        public WordCount() {
        }

        public WordCount(String word, Integer count) {
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public Integer getCount() {
            return count;
        }

        public void setCount(Integer count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
