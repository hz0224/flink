package table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._

case class Stu(id: Int,
               name: String,
               age: Int)

object fromDataDStreamTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
    env.setParallelism(1)
    //fromElements方法从任意的数据集合里创建 DStream
    val inputDStream = env.fromElements("1,lala,20","2,jack,20","3,tom,21")

    val tupleDStream = inputDStream.map { line =>
      val data = line.split(",")
      (data(0).toInt, data(1), data(2).toInt)
    }

    val objDStream =  inputDStream.map{line=>
      val data = line.split(",")
      Stu(data(0).toInt,data(1),data(2).toInt)
    }

    //1 tupleDStream 基于位置 将流转换成表，适用于流中数据类型是元祖类型.
    val table = tableEnv.fromDataStream(tupleDStream,'id,'name,'age)
    //2 tupleDStream 基于case class 将流转换成表，适用于流中数据类型是case class.
    val table2 = tableEnv.fromDataStream(objDStream)

    table.toAppendStream[(Int,String,Int)].print()
    table2.toAppendStream[(Int,String,Int)].print()

    env.execute("from_data_DStream_test")
  }

}
