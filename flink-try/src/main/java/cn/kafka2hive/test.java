package cn.kafka2hive;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.table.functions.ScalarFunction;

public class test {


    public static class DateFormatFunction extends ScalarFunction {
        public static String eval(String ts){

            FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm");
            return format.format(Long.parseLong(ts));
        }
    }
    public static void main(String[] args) {
        System.out.println(DateFormatFunction.eval("1650199652270"));


    }
}
