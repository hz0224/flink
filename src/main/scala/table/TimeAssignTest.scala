package table

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

case class Stu2(id: Int,
               name: String,
               age: Int,
               timeStamp: Long
              )

//时间属性的设置.
object TimeAssignTest {

  def main(args: Array[String]): Unit = {
    testProcessTime()
  }


  def testProcessTime()={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //处理时间语义下不需要设置watermark
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)  //设置时间属性为处理时间，默认也是处理时间
    val tableEnv = StreamTableEnvironment.create(env)

    val inputDStream = env.readTextFile("C:\\Users\\Administrator\\Desktop\\stu.txt")

    val stuDStream = inputDStream.map{line=>
      val data = line.split(",")
      Stu2(data(0).toInt,data(1),data(2).toInt,data(3).toLong)
    }

    //流转换为表，在这一步设置时间属性.   指定处理时间不需要提取时间戳和设置watermark
    val table = tableEnv.fromDataStream(stuDStream,'id,'name,'age,'timeStamp as 'ts,'pt.proctime)   //相当于新增(附加)了一个新的逻辑字段pt
    table.printSchema()

    tableEnv.createTemporaryView("stu",table)

    println(System.currentTimeMillis())

    //pt是处理时间，当前时间减去8个小时。
    tableEnv.sqlQuery("select id,age,ts,pt from stu").toAppendStream[Row].print()

    env.execute("test_process_time")
  }

  def testEventTime()={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)  //设置时间语义为事件时间

    val tableEnv = StreamTableEnvironment.create(env)
    val inputDStream = env.readTextFile("C:\\Users\\Administrator\\Desktop\\stu.txt")

    val stuDStream = inputDStream.map{line=>
      val data = line.split(",")
      Stu2(data(0).toInt,data(1),data(2).toInt,data(3).toLong)
    }.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Stu2](Time.seconds(2L)) {
      override def extractTimestamp(t: Stu2): Long = t.timeStamp * 1000L
    })//分配时间戳和设置水位，即使后面写sql也要先分配时间戳和设置水位，除非采用直接connect的方式注册表。

    //将流转换为表,在设置字段时附加一个事件时间字段.
    //附加一个事件时间，由上面步骤提取的时间戳得到，注意：et这个字段是附加的，原来表结构里没有这个字段。
    val table = tableEnv.fromDataStream(stuDStream,'id,'name,'age,'timeStamp as 'ts,'et.rowtime)
    //val table = tableEnv.fromDataStream(stuDStream,'id,'name,'age,'timeStamp.rowtime as 'et)
    //也可以这样，直接把timeStamp转换为rowtime，这个值和上面一行的'et.rowtime是一样的，这是因为et.rowtime本就是从timeStamp转换过来的。

    //注册表
    tableEnv.createTemporaryView("stu",table)
    table.printSchema()//打印表结构

    //et的值比ts表示的时间早了8个小时，这就是ts和et的区别。et值和watermark和设置的延迟没有关系，就仅仅表示数据的事件时间。
    tableEnv.sqlQuery("select id,name,age,ts,et from stu").toAppendStream[Row].print()

    env.execute("test_event_time")
  }
}
