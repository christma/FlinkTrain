package transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import partitioner.StringPartitioner;

public class PartitionerApp {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        DataStreamSource<Access> source = env.addSource(new MPFuncation.AccessSourceV2());

        System.out.println(source.getParallelism());


        source.map(new MapFunction<Access, Tuple2<String, Access>>() {
                    @Override
                    public Tuple2<String, Access> map(Access access) throws Exception {
                        return Tuple2.of(access.getDomain(), access);
                    }
                }).partitionCustom(new StringPartitioner(), 0)
                .map(new MapFunction<Tuple2<String, Access>, Access>() {
                    @Override
                    public Access map(Tuple2<String, Access> accessTuple2) throws Exception {
                        System.out.println(Thread.currentThread() + " --> " + accessTuple2);
                        return accessTuple2.f1;
                    }
                }).print();

        source.print();


        try {
            env.execute("PartitionerApp");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
