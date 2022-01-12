import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Int;

public class Flink2ClickHouseApp {


    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);


        source.map(new MapFunction<String, Tuple2<Integer, String>>() {
            @Override
            public Tuple2<Integer, String> map(String value) throws Exception {
                String[] splits = value.split(",");
                return Tuple2.of(Integer.valueOf(splits[0].trim()), splits[1].trim());
            }
        }).addSink(JdbcSink.sink(
                "insert into default.user values(?,?)",
                (stamp, x) -> {
                    stamp.setInt(0, x.f0);
                    stamp.setString(1, x.f1);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(2)
                        .withBatchIntervalMs(4000)
                        .build()
                ,
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .withUrl("jdbc:clickhouse://localhost:8123")
                        .build()
        ));


        env.execute("Flink2ClickHouseApp");

    }
}
