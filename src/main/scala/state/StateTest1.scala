package state

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

case class SensorReading(id : Int, temperature: Double, ts: Long)

//状态编程测试1
object StateTest1 {

  def main(args: Array[String]): Unit = {
    //要求同一个温度传感器前后两条数据的年龄差不能超过5，否则打印出来。因此需要将前一条数据保存起来，保存成一个状态，让后一条数据使用。
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputDStream = env.socketTextStream("39.105.49.35",9999)
    val sensorReadingDStream =  inputDStream.map{line=>
      val data = line.split(" ")
      SensorReading(data(0).toInt, data(1).toDouble, data(2).toLong)
    }
    val value = sensorReadingDStream.keyBy(_.id).flatMap(new MyFlatMapper).print()
    env.execute("state_test1")
  }

}


//[SensorReading,(Int,Double,Double,Double)]  SensorReading是输入类型，(Int,Double,Double,Double)是输出类型
//分别代表传感器id，上一次的温度值，这一次的温度值，两者的差值
//状态是和算子绑定在一块的，是算子的功能，flatMap算子默认是没有保存状态功能的，这里我们给它加上这一功能。
class MyFlatMapper extends RichFlatMapFunction[SensorReading,(Int,Double,Double,Double)]{
  var lastTempState: ValueState[Double] = null
  //定义一个状态来判断是否是本分组第一次调用
  var flagState : ValueState[Boolean] = null
  override def open(parameters: Configuration) = {
    //定义一个Value类型的状态来保存上一条数据的温度值. 在 open方法里完成初始化()，也可以用lazy修饰定义在类里面不定义在open方法里。
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last_temperature",classOf[Double])) //初始化后默认值为 0.0
    flagState = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is_first_use",classOf[Boolean])) //初始化后默认值为 false
    //lastTempState.update(-100)
    // 错误，update方法是更新记录的，在open方法调用时是没有记录的，因此在open方法里不能调用update方法去修改状态，只能在flatMap方法里调用update.
  }

  //相比于map，flatMap有更强大的功能，map方法是必须要返回一条数据，也只能返回一条数据，方法强制要求。
  //flatMap方法的返回值为Unit，它靠 collector对象返回数据，并且collector可以调用多次。
  //可以返回数据也可以不返回数据，也可以调用多次返回多条数据(flatMap一对多的实现)，但返回数据的格式要相同。
  override def flatMap(current: SensorReading, collector: Collector[(Int, Double, Double, Double)]) = {
      //判断是否是本组的第一条数据
      if(flagState.value() == false) {
         println("本组的第一条数据")
         lastTempState.update(current.temperature)
         flagState.update(true)
      }else{
        //获取状态里存储的上条数据的温度值
        val lastTemp = lastTempState.value()
        //求差
        val diff = (current.temperature - lastTemp).abs
        //更新状态
        lastTempState.update(current.temperature)
        if(diff > 5)
          collector.collect((current.id,lastTemp,current.temperature,diff))
      }
  }

}