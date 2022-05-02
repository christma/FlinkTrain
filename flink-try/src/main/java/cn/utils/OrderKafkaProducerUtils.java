package cn.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.Properties;

public class OrderKafkaProducerUtils {

    static Producer<String, String> producer;

    public static void init() {
        Properties props = new Properties();
        //此处配置的是kafka的端口
        props.put("bootstrap.servers", "localhost:9092");
//        配置value的序列化类
        //配置value的序列化类
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //配置key的序列化类
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("producer.type", "async");
        //request.required.acks
        //0, which means that the producer never waits for an acknowledgement from the broker (the same behavior as 0.7). This option provides the lowest latency but the weakest durability guarantees (some data will be lost when a server fails).
        //1, which means that the producer gets an acknowledgement after the leader replica has received the data. This option provides better durability as the client waits until the server acknowledges the request as successful (only messages that were written to the now-dead leader but not yet replicated will be lost).
        //-1, which means that the producer gets an acknowledgement after all in-sync replicas have received the data. This option provides the best durability, we guarantee that no messages will be lost as long as at least one in sync replica remains.
        props.put("request.required.acks", "-1");
        producer = new KafkaProducer<>(props);
    }

    public static Producer getProducer() {
        if (producer == null) {
            init();
        }
        return producer;
    }

}
