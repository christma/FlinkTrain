import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceApp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);

        System.out.println(source.getParallelism());

        SingleOutputStreamOperator<String> filter = source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return !"NN".equals(s);
            }
        });

        System.out.println(filter.getParallelism());
        filter.print();

        env.execute("SourceApp");
    }
}
