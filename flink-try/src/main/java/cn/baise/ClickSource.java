package cn.baise;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Random;

public class ClickSource implements ParallelSourceFunction<Event> {
    Boolean runing = Boolean.TRUE;
    private final Random rd = new Random();

    String[] urls = {"A", "B", "C", "D", "E", "F", "G", "H"};

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        while (runing) {
            sourceContext.collect(
                    new Event("name" + rd.nextInt(5)
                            , urls[rd.nextInt(urls.length - 1)]
                            , System.currentTimeMillis()));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        runing = Boolean.FALSE;
    }
}
