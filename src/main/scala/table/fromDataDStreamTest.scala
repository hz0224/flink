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

    //1 tupleDStream 基于位置 将流转换成表，适用于流中数据类型是元祖类型，字段名称自定义即可。
    val table = tableEnv.fromDataStream(tupleDStream,'id,'name,'age)

    //2 objDStream 基于case class 将流转换成表，适用于流中数据类型是case class.
    val table2 = tableEnv.fromDataStream(objDStream) //表里的字段名称和顺序 和 case class一样。

    //3 基于case class 将流转换成表并改变字段顺序和名称，适用于流中数据类型是case class
    val table3 = tableEnv.fromDataStream(objDStream,'name,'id as 'no,'age as 'a)
    //需要使用case class中的属性名来提取字段，由于指定了属性名，因此顺序可以随意指定，并且可以使用 as 指定别名。

    //4 基于case class的位置定义
    val table4 = tableEnv.fromDataStream(objDStream,'no,'n)
    //由于并没有使用case class中的属性名采用的是自定义名称，因此不能改变顺序。 no 就是id，n就是name，age没写就是省略不要。

    table.toAppendStream[(Int,String,Int)].print()
    table2.toAppendStream[(Int,String,Int)].print()
    table3.toAppendStream[(String,Int,Int)].print()
    table4.toAppendStream[(Int,String)].print()

    env.execute("from_data_DStream_test")


  }

}
