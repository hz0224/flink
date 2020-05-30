package window

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.util.Collector
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row
object Test {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val inputDStream = env.socketTextStream("39.105.49.35",9999)

    val lineSplitDStream = inputDStream.map { line =>
      val data = line.split(",")
      val ts = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(data(1)).getTime
      (data(0), ts,data(1), 1L)
    }

    val lineDStreamWithWatermarks = lineSplitDStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long, String, Long)](Time.milliseconds(0L)) {
      override def extractTimestamp(t: (String, Long, String, Long)): Long = {
        t._2
      }
    })

    val tableEnv = TableEnvironment.getTableEnvironment(env)
    //如果是case class的话，就只能够使用对象属性名作为字段名，无法再修改.
    tableEnv.registerDataStream("test",lineDStreamWithWatermarks,'word,'ts.rowtime,'date,'num)
    val sql =
      s"""
         |select
         |    HOP_START(ts, INTERVAL '2' SECOND, INTERVAL '4' SECOND),
         |    HOP_END(ts, INTERVAL '2' SECOND, INTERVAL '4' SECOND),
         |    count(*)
         |from
         |    test
         |group by
         |    HOP(ts, INTERVAL '2' SECOND, INTERVAL '4' SECOND)
      """.stripMargin

    val resultSqlTable = tableEnv.sqlQuery(sql)
    tableEnv.toRetractStream[Row](resultSqlTable).print()
    env.execute("test")
  }

}


//分区开窗口
class MyProcessFunction extends ProcessWindowFunction[(String,Long,String,Long), String,String, TimeWindow] {
  override def process(key: String, context: Context, elements: Iterable[(String,Long,String,Long)], out: Collector[String]): Unit = {
    val start = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(context.window.getStart))
    val end = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(context.window.getEnd))
    val currentWatermark = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(context.currentWatermark))


    println("开始-----" + start)
    var count = 0
    // 遍历，获得窗口所有数据
    for (e <- elements) {
      println(e)
      count += 1
    }
    println("结束-----" + end)
    print("当前水位-----" + currentWatermark)
    out.collect(s"Window ${context.window} , count : ${count}")
  }
}

//不分区开窗口
class MyProcessFunction2 extends ProcessAllWindowFunction[(String,Long,String,Long),String,TimeWindow]{
  override def process(context: Context, elements: Iterable[(String, Long, String, Long)], out: Collector[String]): Unit = {
    val start = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss").format(new Date(context.window.getStart))
    val end = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss").format(new Date(context.window.getEnd))
    println("开始-----" + start)
    var count = 0
    // 遍历，获得窗口所有数据
    for (e <- elements) {
      println(e)
      count += 1
    }
    println("结束-----" + end)
    out.collect(s"Window ${context.window} , count : ${count}")
  }
}


