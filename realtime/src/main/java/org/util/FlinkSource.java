package org.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.admin.Config;

import java.util.Properties;

public class FlinkSource {

    public static SourceFunction<String> getKafkaSource(String groupid, String topic) {


        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", groupid);


        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), props);
    }
}
