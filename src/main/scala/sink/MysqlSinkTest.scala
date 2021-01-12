package sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

object MysqlSinkTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputDStream = env.socketTextStream("39.105.49.35",9999)
    val wordCountDStream = inputDStream.flatMap(_.split(" ")).map((_,1)).keyBy(_._1).sum(1)

    wordCountDStream.addSink(new MysqlSink)
    env.execute("mysql_sink_test")
  }
}

//自定义sink，传入泛型，即调用addSink方法那个 DataStream里的类型.
//也可以将数据库的配置信息使用构造器传过来
class MysqlSink() extends RichSinkFunction[(String,Int)]{
  //定义连接，预编译语句
  var connnection: Connection = null
  var insertStmt: PreparedStatement = null

  //初始化方法，主要做一些对象的初始化操作，随着对象的创建而调用，只调用一次
  override def open(parameters: Configuration) = {
     connnection = DriverManager.getConnection("jdbc:mysql://39.105.49.35:3306/test","root","17746371311")
     val sql =
       """
         |insert into word_count
         |(word,count)
         |values(?,?)
         |on duplicate key update
         |    word = values(word),
         |    count = values(count)
       """.stripMargin
     insertStmt = connnection.prepareStatement(sql)
  }

  //每条数据都会调用，定义对数据的处理逻辑。参数就是输入的数据类型.
  override def invoke(value: (String, Int)) = {
    insertStmt.setString(1,value._1)
    insertStmt.setInt(2,value._2)
    insertStmt.execute()
  }

  //收尾方法，主要做一些对象资源的关闭操作，随着对象的销毁而调用，只调用一次
  override def close() = {
    insertStmt.close()
    connnection.close()
  }


}
