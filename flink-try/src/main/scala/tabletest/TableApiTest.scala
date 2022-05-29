package tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.tableConversions
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.descriptors._
import org.apache.flink.types.Row

object TableApiTest {
  def main(args: Array[String]): Unit = {


    val filePath = "/Users/apple/IdeaProjects/FlinkTrain/flink-try/src/main/resources/sensor.txt"


    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val tEnv = TableEnvironment.create(settings)

    tEnv.connect(new FileSystem().path(filePath))
      .withFormat(new OldCsv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      ).createTemporaryTable("inputTable")


    val result = tEnv.executeSql("select * from inputTable")
    result.print();

//    env.execute("TableApiTest")


  }
}