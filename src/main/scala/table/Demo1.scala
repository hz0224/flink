package table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._

case class Student(id: Int,
                   name: String,
                   age: Int)

//认识Flink SQL的大致流程， DataStream 和 Table对象的互相转换，混搭使用。
object Demo1 {

  def main(args: Array[String]): Unit = {
    //流式处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //基于env创建表环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val inputDStream = env.readTextFile("C:\\Users\\Administrator\\Desktop\\stu.txt")
    val studentDStream = inputDStream.map{line=>
      val data = line.split(",")
      Student(data(0).toInt,data(1),data(2).toInt)
    }
    //基于tableEnv，将流转换成表,所谓的table api即是使用table对象调用一系列方法
    val dataTable = tableEnv.fromDataStream(studentDStream)  //由于studentDStream里面是一个样例类类型，因此转换成的Table对象天然带有schema
    //table对象的具体实现
    println(dataTable.getClass) //org.apache.flink.table.api.internal.TableImpl

    //调用table api进行 table对象的一系列转换
    val resultTable = dataTable
                  .select("id,name,age")
                  .where("id > 2")
                  .where("name = 'tom'")  //也可以写filter方法,where方法底层调用的就是filter方法
    //注意：一个where方法里只能写一个过滤条件

    //table对象不能直接打印,需要将table对象转换成流对象后才能打印。由于 studentDataStream流中的数据是源源不断来的，studentDataStream流中每
    //来一条数据,经过各种转换后table对象中就多一条数据，最后将源源不断来的数据写到追加流中打印出来。
    val resultDStream: DataStream[(Int, String, Int)] = resultTable.toAppendStream[(Int,String,Int)]    //打印的table对象有几列，泛型就是几元祖
    //注意：toAppendStream方法需要隐式转换  org.apache.flink.table.api._ 和 org.apache.flink.streaming.api.scala._

    //打印
    resultDStream.print()

    //以上步骤还可以通过sql实现
    //1 将table对象注册成表
    tableEnv.createTemporaryView("data_table",dataTable)  //createTemporaryView可以将DataStream或Table对象注册成一张临时视图。
    //2 查询返回table对象
    val table: Table = tableEnv.sqlQuery(
      """
        |select
        |   id,
        |   name,
        |   age
        |from
        |   data_table
        |where
        |   id > 2 and name = 'tom'
      """.stripMargin)

    //3 将table对象转换为DStream然后打印结果
    val resultDStream2 = table.toAppendStream[(Int,String,Int)]
    resultDStream2.print()

    env.execute("demo1")
  }

}
