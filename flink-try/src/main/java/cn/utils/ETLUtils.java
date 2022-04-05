package cn.utils;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ETLUtils {

    public static void sendKafka(Producer producer, String topic, String values) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, values);
        producer.send(record, new MyProducerCallBack());
    }

    private static class MyProducerCallBack implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (null != e) {
                e.printStackTrace();
                return;
            }
            System.out.println("时间戳: " + recordMetadata.timestamp() + ", 主题: " + recordMetadata.topic() + ", 分区 :" + recordMetadata.partition() + "位移: " + recordMetadata.offset());
        }
    }
}
