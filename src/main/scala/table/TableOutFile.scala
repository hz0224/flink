package table

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http2.Http2Exception.StreamException
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Schema}
import org.apache.flink.table.types.DataType

object TableOutFile {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)
    val inputDStream = env.readTextFile("C:\\Users\\Administrator\\Desktop\\stu.txt")
    val tupleDStream = inputDStream.map{line=>
      val data = line.split(",")
      (data(0).toInt, data(1),data(2).toInt)
    }
    val table = tableEnv.fromDataStream(tupleDStream,'id,'name,'age)
    tableEnv.createTemporaryView("stu",table)
    val resultTalbe = tableEnv.sqlQuery(
      """
        |select
        |   name,
        |   age
        |from
        |  stu
        |where
        |  id > 3
      """.stripMargin)

    //写出到文件
    //连接外部系统
    val descriptor = tableEnv.connect(new FileSystem().path("C:\\Users\\Administrator\\Desktop\\out.txt"))
    descriptor.withFormat(new OldCsv)
    val schema = new Schema
    schema.field("name",DataTypes.STRING).field("age",DataTypes.INT)
    descriptor.withSchema(schema)
    descriptor.createTemporaryTable("output_table")
    //可见我们在连接外部系统注册表时无论是输入表还是输出表步骤都是一样的，是根据后面执行的操作来确定该表是输入表还是输出表的。
    //后面我们对该表执行的是 insertInto方法可见该表是一个输出表

    resultTalbe.insertInto("output_table")  //此时的写入只支持追加流，不支持撤回流.
    env.execute("table_out_file")
  }
}
