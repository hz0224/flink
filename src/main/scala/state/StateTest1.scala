package state

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala._

case class SensorReading(id : Int, temperature: Double, ts: Long)

//状态编程测试1
object StateTest1 {

  def main(args: Array[String]): Unit = {
    //要求同一个温度传感器前后两条数据的年龄差不能超过3岁，否则给一个提示。因此需要将前一条数据保存起来，保存成一个状态，让后一条数据使用。
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputDStream = env.socketTextStream("39.105.49.35",9999)
    val sensorReadingDStream =  inputDStream.map{line=>
      val data = line.split(" ")
      SensorReading(data(0).toInt, data(1).toDouble, data(2).toLong)
    }

    val value = sensorReadingDStream.keyBy(_.id).map(new MyMapper)

    value.filter{((id,lastTemp,currentTemp,diff))=>
      true
    }



  }

}


//[SensorReading,(Int,Double,Double)]  SensorReading是输入类型，(Int,Double,Double,Double)是输出类型
//分别代表传感器id，上一次的温度值，这一次的温度值，两者的差值
class MyMapper extends RichMapFunction[SensorReading,(Int,Double,Double,Double)]{
  //定义一个Value类型的状态来保存上一条数据的温度值. 由于采用lazy修饰，该变量会在第一次使用到时进行初始化，即执行下面的代码。
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last_temperature",classOf[Double]))
  override def map(current: SensorReading) = {
    //获取状态里存储的上条数据的温度值
    val lastTemp = lastTempState.value()
    //求差
    val diff = (current.temperature - lastTemp).abs
    //更新状态
    lastTempState.update(current.temperature)
    if(diff > 10)
        (current.id,lastTemp,current.temperature,diff)
    else null
  }
}