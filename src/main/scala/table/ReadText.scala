package table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Schema}

//从文件中读取数据
object ReadText {

  def main(args: Array[String]): Unit = {

    //1 流式处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
    val filePath = "C:\\Users\\Administrator\\Desktop\\stu.txt"
    //2 读取文件数据
    //2.1 创建表描述器
    val tableDescriptor = tableEnv.connect(new FileSystem().path(filePath))
    //2.2 定义schema
    val schema = new Schema()
    schema.field("id",DataTypes.INT).field("name",DataTypes.STRING).field("age",DataTypes.INT)
    //2.3 表描述器添加schema
    tableDescriptor.withFormat(new OldCsv) //定义数据格式
                    .withSchema(schema)
    //2.4 表描述器创建表
    tableDescriptor.createTemporaryTable("stu")  //在表环境 Catalog中创建一张表
    //打印  需要先获得Table对象

    val table: Table = tableEnv.from("stu")    //from方法从表环境里获得一张表转换为Table对象.
    val resultDStream = table.toAppendStream[(Int,String,Int)]
    resultDStream.print()

    env.execute("read_text")
  }
}
