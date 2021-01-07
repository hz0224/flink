package table

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http2.Http2Exception.StreamException
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._

object TableOutMysql {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)
    val inputDStream = env.fromElements("1,jack,20","2,lala,18","3,mack,20","4,tom,19","5,jerry,19")

    val tupleDStream = inputDStream.map{line=>
      val data = line.split(",")
      (data(0).toInt,data(1),data(2).toInt)
    }
    val table = tableEnv.fromDataStream(tupleDStream,'id,'name,'age)
    tableEnv.createTemporaryView("stu",table)

    val resultTable = tableEnv.sqlQuery(
      """
        |select
        |   age,
        |   count(1) cnt
        |from
        |   stu
        |group by
        |   age
      """.stripMargin)

    //输出到mysql，FlinkSQL并没有提供连接器connect,需要使用DDL的方式去连接mysql
    val sinkDDL =
      """
        |create table output_table(
        |   age int,
        |   cnt bigint
        |)with (
        |   'connector.type' = 'jdbc',
        |   'connector.url' = 'jdbc:mysql://39.105.49.35:3306/test',
        |   'connector.table' = 'test_table',
        |   'connector.driver' = 'com.mysql.jdbc.Driver',
        |   'connector.username' = 'root',
        |   'connector.password' = '17746371311'
        |)
      """.stripMargin

    //由上面的sql语句知这是一个撤回流，那么写入到mysql中时会实现无则插入，有则修改的功能。那么要实现此功能肯定需要一个key来查询数据库。
    //这个key就是 group by 后的字段，可以是多个字段(组合key)。因此在设计mysql表时要将group by 后的字段设置为主键，这样可以保证达到理想的效果。

    tableEnv.sqlUpdate(sinkDDL)  //连接mysql并在flink环境中注册 output_table表。
    //在flink环境中注册表 output_table ，注意并不是在mysql里创建表，mysql里要提前创建好输出表 test_table: create table test_table(age int,cnt bigint);
    //以上代码就相当于一下代码(flinksql并没有提供对mysql的连接器)
    /*tableEnv
      .connect(new Mysql(...))
    .withFormat(...)
    .withSchema(new Schema.field("age",int).field("cnt",bigint))
    .createTemporaryTable("output_table")*/

    resultTable.insertInto("output_table")
    env.execute("table_out_mysql")
  }

}
