package org.app.dim;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.util.FlinkSource;


public class DimApp {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // checkpoint
        env.enableCheckpointing(3000);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(new Path("./checkpoint/dimapp"));
        env.getCheckpointConfig().setCheckpointTimeout(20 * 1000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);


        DataStreamSource<String> source = env.addSource(FlinkSource.getKafkaSource("DimApp", "ods_db"));
        source.print();


        try {
            env.execute("DimApp");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
