package cn.utils;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class CustomerDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {

//
//    SourceRecord{sourcePartition={server=mysql_binlog_source},
//            sourceOffset={ts_sec=1653206373, file=mysql-bin.000003, pos=154}}

//    ConnectRecord{topic='mysql_binlog_source.flinktrain.test', kafkaPartition=null,
//            key=Struct{item=G,window_start=2022-05-21 04:32:30.0},

//        keySchema=Schema{mysql_binlog_source.flinktrain.test.Key:STRUCT},
//        value=Struct{after=Struct{item=G,price=612,biz_time=2022-05-21 17:32,window_start=2022-05-21 04:32:30.0},
//        source=Struct{version=1.5.2.Final,connector=mysql,name=mysql_binlog_source,ts_ms=1653206373413,
//            snapshot=last,
//            db=flinktrain,
//            table=test,
//            server_id=0,
//            file=mysql-bin.000003,
//            pos=154,row=0},
//            op=r,ts_ms=1653206373413},
//        valueSchema=Schema{mysql_binlog_source.flinktrain.test.Envelope:STRUCT},
//        timestamp=null,
//                headers=ConnectHeaders(headers=)}


    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        JSONObject result = new JSONObject();
        String topic = sourceRecord.topic();
        String[] split = topic.split("\\.");
        result.put("db", split[1]);
        result.put("table", split[2]);

        Struct value = (Struct) sourceRecord.value();
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if (before != null) {
            Schema schema = before.schema();
            List<Field> fields = schema.fields();
            for (Field field: fields) {
                beforeJson.put(field.name(), before.get(field));
            }
        }
        result.put("before", beforeJson);

        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if (after != null) {
            Schema schema = after.schema();
            List<Field> fields = schema.fields();
            for (Field field: fields) {
                afterJson.put(field.name(), after.get(field));
            }
        }
        result.put("after", afterJson);

        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        result.put("op", operation);


        collector.collect(result.toString());

    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
