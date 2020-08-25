package table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Schema}

/**
  * 表的转换
  */
object TableTransform {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)
    val filePath = "C:\\Users\\Administrator\\Desktop\\stu.txt"
    val tableDescriptor = tableEnv.connect(new FileSystem().path(filePath))
    val schema = new Schema()
    schema.field("id",DataTypes.INT).field("name",DataTypes.STRING).field("age",DataTypes.INT)
    tableDescriptor.withFormat(new OldCsv)
                    .withSchema(schema)
    tableDescriptor.createTemporaryTable("stu")
    val table = tableEnv.from("stu")

    //可以用一个单引号加字段名来表示提取这个字段，调用的是 select(fields: Expression*) 这个方法。
    val resultTable = table.select('id,'name,'age).where('name === "lala") //在Expression中使用 === 来表示字符串相等。
    val resultDStream = resultTable.toAppendStream[(Int,String,Int)]
    resultDStream.print("追加流")

    //聚合操作的写法
    val groupTable = table.groupBy('age).select('age,'id.count as 'count)
    groupTable.toRetractStream[(Int,Long)].print("撤回流")   //count函数是累加的，每条数据进来都要修改之前的count值，因此不能采用追加流，而是采用撤回流。
    //注意观察打印结果

    env.execute("table_transform")
  }
}
