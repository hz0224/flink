package process_function

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

case class SensorReading3(id : Int, temperature: Double, ts: Long)

//测试 processFunction 定时器，如果温度在10秒内连续上升则输出信息
object OnTimerTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputDStream = env.socketTextStream("39.105.49.35",9999)
    val sensorReadingDStream =  inputDStream.map{line=>
      val data = line.split(" ")
      SensorReading3(data(0).toInt, data(1).toDouble, data(2).toLong)
    }
    sensorReadingDStream.keyBy(_.id).process(new TempIncreWarning(10000L)).print()

    env.execute("on_timer_test")
  }
}

//如果温度在10秒内连续上升则输出信息
class TempIncreWarning(interval: Long) extends KeyedProcessFunction[Int,SensorReading3,String]{
  //定义状态，保存上一个温度值
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last_temp",classOf[Double]))
  //定义状态，保存注册定时器的时间戳用于删除定时器
  lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer_ts",classOf[Long]))

  //每一条数据都会调用的方法
  override def processElement(value: SensorReading3, ctx: KeyedProcessFunction[Int, SensorReading3, String]#Context, out: Collector[String]) = {
      val lastTemp = lastTempState.value()
      val timerTs = timerTsState.value()
      lastTempState.update(value.temperature)

      //如果温度上升且没有定时器注册，那么注册当前时间加10s后的定时器
      if(value.temperature > lastTemp && timerTs == 0){
          val ts = ctx.timerService().currentProcessingTime() + interval
          ctx.timerService().registerProcessingTimeTimer(ts)
          //保存定时器设置的时间戳
          timerTsState.update(ts)
      }else if(value.temperature < lastTemp){
          //当存在上一个定时器时才删除
          if(timerTs != 0){ //其实不加这个if也可以，deleteProcessingTimeTimer删除一个不存在的定时器时是不会报错的。
            ctx.timerService().deleteProcessingTimeTimer(timerTs)
            timerTsState.clear() //清空就是变成初始值，long的初始值就是0
          }
      }else{} //温度连续上升且已经有定时器时不进行任何操作，等定时器触发就行了。
  }

  //定时器触发方法
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Int, SensorReading3, String]#OnTimerContext, out: Collector[String]) = {
     out.collect("传感器 " + ctx.getCurrentKey + " 温度连续 " + interval/1000 + " 秒上升")
     //清空定时器时间戳状态
     timerTsState.clear()
  }
}
