import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

public class FlinkCEPApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.fromElements(
                "001,78.78.78.79,success,1641957849",
                "001,78.78.78.79,failure,1641957849",
                "002,78.78.78.79,failure,1641957850",
                "003,78.78.78.79,success,1641957844",
                "002,78.78.78.79,failure,1641957851",
                "003,78.78.78.79,success,1641957852",
                "002,78.78.78.79,failure,1641957860",
                "002,78.78.78.79,success,1641957887"
        );

        KeyedStream<Access, String> stream = source.map(new MapFunction<String, Access>() {
            @Override
            public Access map(String value) throws Exception {
                String[] splits = value.split(",");

                Access access = new Access();
                access.id = splits[0];
                access.ip = splits[1];
                access.state = splits[2];
                access.date = splits[3];
                return access;
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Access>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(Access element) {
                return Long.parseLong(element.date);
            }
        }).keyBy(x -> x.id);
        stream.print();

        // 定义CEP规则

        Pattern<Access, Access> pattern = Pattern.<Access>begin("start")
                .where(new SimpleCondition<Access>() {
                    @Override
                    public boolean filter(Access value) throws Exception {
                        return value.state.equals("failure");
                    }
                }).next("next")
                .where(new SimpleCondition<Access>() {
                    @Override
                    public boolean filter(Access value) throws Exception {
                        return value.state.equals("failure");
                    }
                }).within(Time.seconds(3));

        PatternStream<Access> patternStream = CEP.pattern(stream, pattern);

        patternStream.select(new PatternSelectFunction<Access, AccessMsg>() {
            @Override
            public AccessMsg select(Map<String, List<Access>> map) throws Exception {
                Access first = map.get("start").get(0);
                Access second = map.get("next").get(0);
                AccessMsg accessMsg = new AccessMsg();
                accessMsg.id = first.id;

                accessMsg.first = first.date;
                accessMsg.second = second.date;
                accessMsg.msg = "出现连续两次登陆失败...";
                return accessMsg;
            }
        }).print();

        env.execute("FlinkCEPApp");


    }

    static class AccessMsg {
        private String id;
        private String first;
        private String second;
        private String msg;


        @Override
        public String toString() {
            return "AccessMsg{" +
                    "id='" + id + '\'' +
                    ", first='" + first + '\'' +
                    ", second='" + second + '\'' +
                    ", Msg='" + msg + '\'' +
                    '}';
        }
    }

    static class Access {
        private String id;
        private String ip;
        private String state;
        private String date;

        @Override
        public String toString() {
            return "Access{" +
                    "id='" + id + '\'' +
                    ", ip='" + ip + '\'' +
                    ", state='" + state + '\'' +
                    ", date='" + date + '\'' +
                    '}';
        }
    }


}
