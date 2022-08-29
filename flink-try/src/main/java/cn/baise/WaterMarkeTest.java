package cn.baise;

import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class WaterMarkeTest {
    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new ClickSource()).assignTimestampsAndWatermarks(new CustomWatermarkStrategy()).print();

        env.execute();
    }


    private static class CustomWatermarkStrategy implements WatermarkStrategy<Event> {

        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new CustomBoundedOutofOrdernessGenerator();
        }

        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event event, long l) {
                    return event.timestamp;
                }
            };


        }

        private class CustomBoundedOutofOrdernessGenerator implements WatermarkGenerator<Event> {
            private Long deleyTime = 5000L;
            private Long maxTs = -Long.MAX_VALUE + deleyTime + 1L;

            @Override
            public void onEvent(Event event, long l, WatermarkOutput watermarkOutput) {
                maxTs = Math.max(event.timestamp, maxTs);
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                watermarkOutput.emitWatermark(new Watermark(maxTs - deleyTime - 1L));
            }
        }
    }
}
