package cn.kafka2hive;


import cn.utils.CustomerDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DebeziumSourceFunction<String> source = MySqlSource.<String>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .username("root")
                .password("root")
                .databaseList("flinktrain")
                .tableList("flinktrain.test")
                .deserializer(new CustomerDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        env.addSource(source).print();


        env.execute("FlinkCDC");

    }
}
