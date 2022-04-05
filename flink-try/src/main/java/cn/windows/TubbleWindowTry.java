package cn.windows;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TubbleWindowTry {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE KafkaTable (" +
                "  `bidtime` TIMESTAMP(3)," +
                "  `price` DECIMAL(10, 2)," +
                "  `item` STRING " +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'user_behavior'," +
                "  'properties.bootstrap.servers' = 'localhost:9092'," +
                "  'properties.group.id' = 'one'," +
                "  'scan.startup.mode' = 'latest-offset'," +
                "  'format' = 'json'" +
                ")");

        Table table = tableEnv.sqlQuery("select * from KafkaTable");
        tableEnv.toAppendStream(table, Row.class).print();

    }

}
