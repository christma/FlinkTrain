package sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import transformation.Access;
import utils.MySQLUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;


public class MySQLSink extends RichSinkFunction<Tuple2<String, Double>> {

    Connection connection;
    PreparedStatement insertPstm;
    PreparedStatement updatePstm;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = MySQLUtils.getConnection();
        insertPstm = connection.prepareStatement("insert into access(domain,traffic)values(?,?)");
        updatePstm = connection.prepareStatement("update access set traffic = ? where domain = ?");
    }

    //insert into access(domain,traffic)values(a.com,10000)

    @Override
    public void close() throws Exception {
        MySQLUtils.close(connection, insertPstm);
        MySQLUtils.close(connection, updatePstm);
    }

    @Override
    public void invoke(Tuple2<String, Double> value, Context context) throws Exception {
        System.out.println("-----invoke---------");
        System.out.println(value.f0 + "  " + value.f1);
        updatePstm.setDouble(1, value.f1);
        updatePstm.setString(2, value.f0);

        updatePstm.execute();

        if (updatePstm.getUpdateCount() == 0) {
            insertPstm.setString(1, value.f0);
            insertPstm.setDouble(2, value.f1);
            insertPstm.execute();
        }

    }

}
