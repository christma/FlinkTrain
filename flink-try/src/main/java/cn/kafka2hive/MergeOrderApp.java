package cn.kafka2hive;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class MergeOrderApp {
    public static void main(String[] args) throws Exception {

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

        env.setParallelism(1);
//        {"uid":"152","add_time":1651300899464,"order_id":"0A2D37B88A8040E8AE6984B7BFC69925"}
        tableEnvironment.executeSql(
                "create table kafka_order_source (\n" +
                        "uid string, \n" +
                        "order_id string, \n" +
                        "add_time bigint \n," +
                        "row_time as TO_TIMESTAMP(FROM_UNIXTIME(add_time/1000)),\n" +
                        "WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND \n" +
                        ")with(\n" +
                        "'connector'='kafka',\n" +
                        "'topic'='order',\n" +
                        "'properties.bootstrap.servers'='localhost:9092',\n" +
                        "'properties.group.id'='flink-consumer001',\n" +
                        "'scan.startup.mode'='latest-offset',\n" +
                        "'format'='json',\n" +
                        "'json.fail-on-missing-field'='false',\n" +
                        "'json.ignore-parse-errors'='true'\n" +
                        ")");


//        {"num":3,"name":"e","goods_id":"514263B5","order_id":"0A2D37B88A8040E8AE6984B7BFC69925","prince":50.0}
        tableEnvironment.executeSql(
                "create table kafka_order_detail_source (\n" +
                        "name string, \n" +
                        "order_id string, \n" +
                        "goods_id string, \n" +
                        "prince double, \n" +
                        "num int, \n" +
                        "row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n" +
                        "WATERMARK FOR row_time AS row_time \n" +
                        ")with(\n" +
                        "'connector'='kafka',\n" +
                        "'topic'='order_detail',\n" +
                        "'properties.bootstrap.servers'='localhost:9092',\n" +
                        "'properties.group.id'='flink-consumer001',\n" +
                        "'scan.startup.mode'='latest-offset',\n" +
                        "'format'='json',\n" +
                        "'json.fail-on-missing-field'='false',\n" +
                        "'json.ignore-parse-errors'='true'\n" +
                        ")");


//        create table test_order (
//                uid VARCHAR(255),
//                order_id varchar(255),
//                add_time TIMESTAMP,
//                name varchar(255),
//                goods_id varchar(255),
//                prince DOUBLE,
//                num int,
//        PRIMARY KEY(order_id)
//)

        tableEnvironment.executeSql("create table test_order(\n" +
                "uid string,\n" +
                "order_id string,\n" +
                "add_time timestamp,\n" +
                "name string,\n" +
                "goods_id string,\n" +
                "prince double,\n" +
                "num int, \n" +
                "PRIMARY KEY (order_id) NOT ENFORCED \n" +
                ")with(\n" +
                "'connector'='jdbc',\n" +
                "'url'='jdbc:mysql://localhost:3306/flinktrain?useSSL=false',\n" +
                "'table-name'='test_order',\n" +
                "'password'='root',\n" +
                "'username'='root'\n" +
                ")");


        tableEnvironment.executeSql("insert into test_order \n" +
//                "create temporary view xxxxx as " +
                "select uid,\n" +
                "os.order_id,\n" +
                "TO_TIMESTAMP(FROM_UNIXTIME(add_time/1000)) as add_time,\n" +
                "name,\n" +
                "ods.goods_id,\n" +
                "ods.prince,\n" +
                "ods.num from \n" +
                "kafka_order_source os \n" +
                "left join \n" +
                "kafka_order_detail_source ods \n" +
                "on os.order_id = ods.order_id \n" +
                "and os.row_time between ods.row_time - INTERVAL '10' MINUTE \n" +
                "and ods.row_time + INTERVAL '10' MINUTE \n");
//
//MINUTE
//        Table table = tableEnvironment.sqlQuery("select * from xxxxx");
//        tableEnvironment.toRetractStream(table, Row.class).print();


//        env.execute("MergeOrderApp");
    }
}
