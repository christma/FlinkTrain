package cn.mock;

import cn.utils.ETLUtils;
import cn.utils.KafkaProducerUtils;
import cn.utils.MockDataUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.Producer;


public class kafkamock {
    Producer producer = KafkaProducerUtils.getProducer();

    void mock(){
        long bidtime = MockDataUtils.bidtime();
        String item = MockDataUtils.item();
        long price = MockDataUtils.price();
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("biz_time",bidtime);
        jsonObject.put("item",item);
        jsonObject.put("price",price);
        String value = JSON.toJSONString(jsonObject);
        System.out.println(value);
        ETLUtils.sendKafka(producer,"flinktry",value);
    }

    public static void main(String[] args) throws InterruptedException {
        kafkamock kafkamock = new kafkamock();
        while (true){
            kafkamock.mock();
            Thread.sleep(5000);
        }
    }






}
