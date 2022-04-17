package cn.kafka2hive;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class FlinkKafkaToHive {

    public static void main(String[] args) throws Exception {

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();



        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env,settings);

        env.setParallelism(1);

        Configuration conf = tableEnvironment.getConfig().getConfiguration();
        conf.setString("table.exec.mini-batch.enabled", "true");
        conf.setString("table.exec.mini-batch.allow-latency", "5s");
        conf.setString("table.exec.mini-batch.size", "5000");
        conf.setString("rest.flamegraph.enabled", "true");

//      {"item":"D","price":917,"biz_time":1649076083575}
        tableEnvironment.executeSql(
                "create table kafka_source (\n" +
                        "item string, \n" +
                        "price int, \n" +
                        "biz_time bigint \n," +
                        "row_time as TO_TIMESTAMP(FROM_UNIXTIME(biz_time/1000)),\n" +
                        "WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND \n" +
                        ")with(\n" +
                        "'connector'='kafka',\n" +
                        "'topic'='flinktry',\n" +
                        "'properties.bootstrap.servers'='localhost:9092',\n" +
                        "'properties.group.id'='flink-consumer001',\n" +
                        "'scan.startup.mode'='earliest-offset',\n" +
                        "'format'='json',\n" +
                        "'json.fail-on-missing-field'='false',\n" +
                        "'json.ignore-parse-errors'='true'\n" +
                        ")");

        tableEnvironment.executeSql("create table mysql_sink(\n" +
                "item string,\n" +
                "price int,\n" +
                "window_start timestamp,\n" +
                "PRIMARY KEY (item,window_start) NOT ENFORCED \n" +
                ")with(\n" +
                "'connector'='jdbc',\n" +
                "'url'='jdbc:mysql://localhost:3306/flinktrain',\n" +
                "'table-name'='test',\n" +
                "'password'='root',\n" +
                "'username'='root'\n" +
                ")");

        tableEnvironment.executeSql("insert into mysql_sink \n" +
                "select item,\n" +
                        "sum(price) as price, \n" +
                        "to_timestamp(DATE_FORMAT(window_start,'yyyy-MM-dd HH:mm:00')) as window_start  \n" +
                        "FROM TABLE(TUMBLE(\n" +
                        "TABLE kafka_source\n" +
                        ", DESCRIPTOR(row_time)\n" +
                        ", INTERVAL '60' SECOND))\n" +
                        "GROUP BY window_start, \n" +
                        "window_end,\n" +
                        "item");

//        tableEnvironment.toRetractStream(table, Row.class).print();

//        env.execute("FlinkKafkaToHive");
    }
}
