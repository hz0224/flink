package com.winbaoxian.bigdata.watermark

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http2.Http2Exception.StreamException
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._

case class Trace(user_id: String, ts: Long, event_type: Int)

object WaterMarkTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val traceDStream =  env.readTextFile("").map{line=>
      val data = line.split(" ")
      Trace(data(0), data(1).toLong, data(2).toInt)
    }
    //traceDStream.assignAscendingTimestamps(_.ts)  //毫米数
    //assignAscendingTimestamps这个时间戳分配器是针对于升序的Event time，由于是有序的，因此不需要设置watermark
    //仅仅是提取事件时间戳，直接由数据的事件时间来触发窗口计算。由于实际中都是乱序数据，因此这个基本上用不到。

    /**
      *assignTimestampsAndWatermarks可以同时分配时间戳和设置延迟时间，里面可以传以下两个类型的assigner(都是接口)
      * 1 AssignerWithPunctuatedWatermarks   不间断assigner
      *     每条数据都会计算一个watermark，比较耗费资源，适用于数据稀疏的场景
      *
      * 2 AssignerWithPeriodicWatermarks    周期性assigner
      *     并不是每条数据都计算一个watermark，而是根据一个周期来计算一个watermark，默认的周期是 200毫秒
      *     适用于数据量大数据密集的场景，这个在实际中用的较多。
      *     我们经常使用的实现类是 BoundedOutOfOrdernessTimestampExtractor
      */
    //返回一个分配了时间戳和延迟的DataStream
    val assignTraceDStream = traceDStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Trace](Time.seconds(3)){ //传入乱序程度(延迟时间)
      override def extractTimestamp(trace: Trace): Long = trace.ts   //返回时间戳，毫秒数
    })

    env.execute("water_mark_test")
  }

}
