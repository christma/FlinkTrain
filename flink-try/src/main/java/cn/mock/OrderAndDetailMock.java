package cn.mock;



import cn.entry.Order;
import cn.entry.OrderDetail;
import cn.entry.OrderMockDataUtils;
import cn.utils.ETLUtils;
import cn.utils.OrderKafkaProducerUtils;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.Producer;

import java.util.Random;

public class OrderAndDetailMock {

    Producer producer = OrderKafkaProducerUtils.getProducer();

    public void mock() {
        String orderId = OrderMockDataUtils.orderId();
        String uid = OrderMockDataUtils.getUid();
        long addTime = OrderMockDataUtils.addTime();

        String goodIdAndPrince = OrderMockDataUtils.goodIdAndPrince();
        String goodId = goodIdAndPrince.split(",")[0];
        Double prince = Double.parseDouble(goodIdAndPrince.split(",")[1]);
        String goodName = goodIdAndPrince.split(",")[2];
        Integer num = OrderMockDataUtils.getNum();

        Order order = new Order(uid, orderId, addTime);
        OrderDetail orderDetail = new OrderDetail(orderId, goodId, goodName, prince, num);

        String JsonOrder = JSONObject.toJSON(order).toString();
        String JsonOrderDetial = JSONObject.toJSON(orderDetail).toString();

        System.out.println(JsonOrder);
      ETLUtils.sendKafka(producer,"order",JsonOrder);

        System.out.println(JsonOrderDetial);
      ETLUtils.sendKafka(producer, "order_detail", JsonOrderDetial);
    }

    public static void main(String[] args) throws Exception {
//        System.out.println(new Random().nextInt(10) * 1000);
        OrderAndDetailMock mock = new OrderAndDetailMock();
        while (true) {
            mock.mock();
            Thread.sleep(new Random().nextInt(10) * 500);
        }
    }

}
