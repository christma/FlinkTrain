package cn.baise;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.runtime.aggregate.AggregateAggFunction;
import org.apache.flink.util.Collector;

public class UrlViewCountAgg implements AggregateFunction<Event, Long, Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(Event event, Long aLong) {
        return aLong + 1;
    }

    @Override
    public Long getResult(Long aLong) {
        return aLong;
    }

    @Override
    public Long merge(Long aLong, Long acc1) {
        return null;
    }


    public static class UrlViewCountResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {

        @Override
        public void process(String s, ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>.Context context, Iterable<Long> iterable, Collector<UrlViewCount> collector) throws Exception {
            long window_start = context.window().getStart();
            long window_end = context.window().getEnd();
            Long count = iterable.iterator().next();
            collector.collect(new UrlViewCount(s, count, window_start, window_end));

        }
    }
}
