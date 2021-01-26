package process_function

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

case class SensorReading2(id : Int, temperature: Double, ts: Long)

object ProcessFunctionTest1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputDStream = env.socketTextStream("39.105.49.35",9999)
    val sensorReadingDStream =  inputDStream.map{line=>
      val data = line.split(" ")
      SensorReading2(data(0).toInt, data(1).toDouble, data(2).toLong)
    }
    sensorReadingDStream.keyBy(_.id).process(new MyKeyedProcessFunction)
    env.execute("process_function_test1")
  }
}

// KeyedProcessFunction<K, I, O>    K是keyedStream里key的类型，I是输入类型，O是输出类型。
class MyKeyedProcessFunction extends KeyedProcessFunction[Int,SensorReading2,String]{
  //KeyedProcessFunction继承自AbstractRichFunction，因此它也具有RichFunction的功能
  var state: ValueState[Double] = null
  override def open(parameters: Configuration) = {
      state = getRuntimeContext.getState(new ValueStateDescriptor[Double]("state",classOf[Double])) //初始化后默认值为 0.0
  }

  //processElement方法本身的返回值为Unit，它靠Collector对象实现输出，可输出也可不输出，也可输出多行。
  //不输出可以实现filter的功能，输出多行可以实现flatMap的功能.
  override def processElement(value: SensorReading2, ctx: KeyedProcessFunction[Int, SensorReading2, String]#Context, out: Collector[String]) = {
      ctx.getCurrentKey   //获取当前key
      ctx.timestamp()     //当前数据所带的时间戳
      ctx.timerService().currentWatermark()   //当前的水位
      ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 60000L) //注册当前 key的事件时间定时器
      ctx.timerService().deleteEventTimeTimer(60000)  //删除当前key的事件时间定时器，传入当时设置的时间戳
  }

  /**
    * 定义定时器触发后的操作,定时器可以设置多个，通过当时设置的时间戳进行区分
    * 所有的定时器触发时都会调用onTimer方法，要想实现对不同的定时器执行不同的操作，可以在onTimer方法里通过判断时间戳实现。
    * @param timestamp  此定时器当时设置的时间戳
    * @param ctx
    * @param out
    * 当水位 >= 定时器注册的事件时间，执行onTimer函数
    */
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Int, SensorReading2, String]#OnTimerContext, out: Collector[String]) = {

  }
}
