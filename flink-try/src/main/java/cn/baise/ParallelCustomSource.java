package cn.baise;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

public class ParallelCustomSource implements ParallelSourceFunction<Integer> {

    private Boolean running = true;
    private final Random rd = new Random();

    @Override
    public void run(SourceContext<Integer> sourceContext) throws Exception {

        while (running) {
            sourceContext.collect(rd.nextInt(10));
            Thread.sleep(1000);
        }

    }

    @Override
    public void cancel() {
        running = false;
    }
}
