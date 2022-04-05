package cn.utils;

import java.util.Properties;

public class FlinkKafkaConsumerUtils {

    public static Properties getConsumerProperties(String brokers, String topic, String groupId) {
        Properties properties = getCommonProperties(topic + groupId);
        properties.setProperty("bootstrap.servers", brokers);
        return properties;
    }

    public static Properties getCommonProperties(String groupId) {
        Properties properties = new Properties();
        properties.setProperty("client.id", String.format("consumer-%s-%d", groupId, System.currentTimeMillis()));
        properties.setProperty("group.id", groupId);
        properties.setProperty("max.partition.fetch.bytes", "3145728");
        properties.setProperty("auto.offset.reset", "latest");
        properties.setProperty("heartbeat.interval.ms", "10000");
        properties.setProperty("flink.partition-discovery.interval-millis", "60000");
        properties.setProperty("session.timeout.ms", "60000");
        properties.setProperty("request.timeout.ms", "65000");
        properties.setProperty("enable.auto.commit", "false");
        properties.setProperty("auto.commit.interval.ms", "30000");
        return properties;
    }
}
