package cn.utils;


import org.apache.commons.lang3.RandomStringUtils;

/**
 * orgId
 * time
 * <p>
 * maskCode
 * tagCode
 * value
 * time
 * quality
 */
public class MockDataUtils {
    private static String[] itemArray = {"A", "B", "C", "D", "E", "F", "G"};

    public static String item() {
        double random = Math.random() * 17;
        return itemArray[(int) (random % itemArray.length)];
    }

    public static long bidtime() {
        return System.currentTimeMillis();
    }


    public static long price() {
        double random = Math.random() * 1000;
        return (long) (random);
    }


    public static void main(String[] args) {
        System.out.println(price());
    }

}
