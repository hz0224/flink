package window

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.watermark.Watermark

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Liu HangZhou on 2020/05/30
  * desc: 深刻理解水位概念
  */
object EventTimeWithWindow {

  private val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {

      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
      env.setParallelism(1)

      //0001,2019-11-12 11:25:00
      val socketDStream = env.socketTextStream("39.105.49.35",9999)

      socketDStream.map{line=>
        val data = line.split(",")
        val ts = dateFormat.parse(data(1)).getTime
        (data(0),ts)
      }/*assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String,Long)](Time.milliseconds(0L)){
        override def extractTimestamp(t: (String, Long)) = t._2
      })*/  //没有设置水位的情况
        .assignTimestampsAndWatermarks(new TimeStampExtractor)
        .keyBy(_._1)
        .window(TumblingEventTimeWindows.of(Time.seconds(3)))
        //.allowedLateness(Time.seconds(5)) // 允许延迟5s之后才销毁计算过的窗口
        .process(new MyProcessWindowFunction3)
        .print()

      env.execute("event_time_wit_window")
  }

}

class TimeStampExtractor extends AssignerWithPeriodicWatermarks[(String, Long)] with Serializable {
  var currentMaxTimestamp = 0L
  val maxOutOfOrderness = 10000L // 最大允许的乱序时间是10s
  var a: Watermark = null

  val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  // 水印 是 当前时间减去10s     先执行extractTimestamp方法再执行getCurrentWatermark方法.
  //其实 getCurrentWatermark方法是在不间断的执行的。
  override def getCurrentWatermark: Watermark = {
    //不能在本方法里进行打印操作，因为 getCurrentWatermark方法会一直执行，影响打印效果。
    a = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
    a
  }

  override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
    println("===================extractTimestamp start=========================")
    //        println("extractTimestamp")
    val timestamp = element._2
    currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
    println("userid：" + element._1)
    println("事件时间戳：" + element._2)
    println("事件时间:" + format.format(element._2))
    println("当前最大时间戳：" + currentMaxTimestamp)
    println("当前最大时间：" + format.format(currentMaxTimestamp))
    val currentWatermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
    //由于 extractTimestamp方法执行后会立即执行 getCurrentWatermark方法进行 当前窗口中的最大时间减去10秒获得当前水位的操作。
    //但是又不能在getCurrentWatermark方法里打印，所以这里就提前(也就几毫秒)将getCurrentWatermark方法里的逻辑执行一遍，方便打印效果。
    //这里currentWatermark变量仅仅为了打印效果，没有进行任何逻辑操作。
    println("当前水位(当前窗口中的最大时间减去10秒)：" + format.format(new Date(currentWatermark.getTimestamp)))
    println("===================extractTimestamp end=========================")
    timestamp
  }
}


class MyProcessWindowFunction3 extends ProcessWindowFunction[(String,Long),String,String,TimeWindow]{
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {

    println("===================process start=========================")
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    println("开始时间：" + dateFormat.format(context.window.getStart))
    println("key：" + key)
    println("窗口中元素的个数：" + elements.size)
    val arr = ArrayBuffer[(String, Long)]()
    for(value <- elements){
      println(value._1, dateFormat.format(new Date(value._2)))
    }
    println("结束时间：" + dateFormat.format(context.window.getEnd))
    println("水位：" + dateFormat.format(context.currentWatermark))
    out.collect(s"Window ${context.window}")
    println("====================process end========================")
  }
}