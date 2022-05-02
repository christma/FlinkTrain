package cn.entry;


import org.apache.commons.lang3.RandomStringUtils;

import java.util.UUID;

/**
 * //orderid int(11) NO 订单id号
 * //goodsid int(11) NO 商品id号
 * //prince double(6,2) NO 单价
 * //num int(11) NO 数量
 */
public class OrderMockDataUtils {


    private static String[] orgIdArray = {"702837020728250368"};

    private static String[] UUIDArray = {
            "114263B1,10,a",
            "214263B2,20,b",
            "314263B3,30,c",
            "414263B4,40,d",
            "514263B5,50,e",
            "614263B6,60,f",
            "714263B7,70,g",
            "814263B8,80,h",
            "914263B9,90,i"
    };

    public static String orderId() {
        return UUID.randomUUID().toString().replace("-", "").toUpperCase();
    }

    public static long addTime() {
        return System.currentTimeMillis();
    }

    public static String goodIdAndPrince() {
        double random = Math.random() * 17;
        return UUIDArray[(int) (random % UUIDArray.length)];
    }

    public static String getUid() {
        return RandomStringUtils.random(3, "12345");
    }

    public static Integer getNum() {
        double random = Math.random() * 10;
        return (int) (random);
    }

    public static void main(String[] args) {
        for (int i = 0; i < 10; i++) {
            System.out.println(getUid());

        }
    }

}
