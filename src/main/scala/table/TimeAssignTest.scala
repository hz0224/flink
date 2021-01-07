package table

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._

case class Stu2(id: Int,
               name: String,
               age: Int,
               timeStamp: Long
              )

//时间属性的设置.
object TimeAssignTest {

  def main(args: Array[String]): Unit = {
    testEventTime()


  }


  def testProcessTime()={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
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
    }.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Stu2](Time.seconds(0)) {
      override def extractTimestamp(t: Stu2): Long = t.timeStamp * 1000L
    })//分配时间戳和设置水位.

    stuDStream.keyBy(_.id).timeWindow(Time.seconds(5)).min("age").print()




    //流转换为表,在设置字段时附加一个事件时间字段.
//    val table = tableEnv.fromDataStream(stuDStream,'id,'name,'age,'timeStamp as 'ts,'et.rowtime) //附加一个事件时间，由上面步骤提取的时间戳得到
//    tableEnv.createTemporaryView("stu",table)
//
//    table.printSchema()
//    tableEnv.sqlQuery("select id,name,age,ts from stu").toAppendStream[(Int,String,Int,Long)].print()


    env.execute("test_event_time")
  }



}
