package app;

import com.alibaba.fastjson.JSON;
import domain.Access;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Int;

public class OsUserCntAppV1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Access> source = env.readTextFile("datas/access.json")
                .map(new MapFunction<String, Access>() {
                    @Override
                    public Access map(String value) throws Exception {

                        try {
                            return JSON.parseObject(value, Access.class);

                        } catch (Exception e) {
                            e.printStackTrace();
                            return null;
                        }
                    }
                }).filter(access -> access != null)
                .filter(new FilterFunction<Access>() {
                    @Override
                    public boolean filter(Access access) throws Exception {
                        return "startup".equals(access.event);
                    }
                });


        source.map(new MapFunction<Access, Tuple3<String, Integer, Integer>>() {
            @Override
            public Tuple3<String, Integer, Integer> map(Access access) throws Exception {
                return Tuple3.of(access.os, access.nu, 1);
            }
        }).keyBy(new KeySelector<Tuple3<String, Integer, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> getKey(Tuple3<String, Integer, Integer> value) throws Exception {
                return Tuple2.of(value.f0, value.f1);
            }
        }).sum(2).print();

        env.execute("OsUserCntAppV1");
    }


}
