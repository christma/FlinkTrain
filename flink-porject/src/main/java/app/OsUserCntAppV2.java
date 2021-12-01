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

public class OsUserCntAppV2 {
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


        source.map(new MapFunction<Access, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Access access) throws Exception {
                return Tuple2.of(access.nu, 1);
            }
        }).keyBy(new KeySelector<Tuple2<Integer, Integer>, Integer>() {
            @Override
            public Integer getKey(Tuple2<Integer, Integer> value) throws Exception {
                return value.f0;
            }
        }).sum(1).print();

        env.execute("OsUserCntAppV2");
    }


}
