package kafka2hdfs;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Kafka2Hdfs {

    private static Logger LOG = LoggerFactory.getLogger(Kafka2Hdfs.class);

    public static void main(String[] args) {
        if (args.length != 3) {
            LOG.error("kafka(server01:9092), hdfs(hdfs://cluster01/data/), flink(parallelism=2) must be exist.");
            return;
        }

        String bootStrapServer = args[0];
        String hdfsPath = args[1];
        int parallelism = Integer.parseInt(args[2]);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000);
        env.setParallelism(parallelism);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> transction = env.addSource(new FlinkKafkaConsumer010<>("test_bll_data",new SimpleStringSchema(),configByKafkaServer(bootStrapServer)));

        BucketingSink<String> sink = new BucketingSink<>(hdfsPath);
        sink.setBucketer(new DateTimeBucketer<>("HH-mm"));// 自定义存储到HDFS上的文件名，用小时和分钟来命名，方便后面算策略

        sink.setBatchSize(1024 * 1024 * 4); // this is 5MB
        sink.setBatchRolloverInterval(1000 * 30); // 30s producer a file into hdfs
        transction.addSink(sink);

    }
    private static Properties configByKafkaServer(String bootStrapServer) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", bootStrapServer);
        props.setProperty("group.id", "test_bll_group");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }
}
