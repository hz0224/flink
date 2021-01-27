package process_function

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

case class SensorReading4(id : Int, temperature: Double, ts: Long)

//使用 ProcessFunction和侧输出流完成分流操作
object SideOutPutTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputDStream = env.socketTextStream("39.105.49.35",9999)
    val sensorReadingDStream =  inputDStream.map{line=>
      val data = line.split(" ")
      SensorReading4(data(0).toInt, data(1).toDouble, data(2).toLong)
    }
    //allDStream里面有两条流，一条是主流，一条是侧输出流，主流里并不包含侧输出流
    val allDStream = sensorReadingDStream.process(new MySplitTempProcessor(30.0))  //由于是分流，直接对所有的数据分，因此不需要keyBy
    allDStream.print("主流")  //allDStream直接用的话里面都是主流里的数据
    //要想得到侧输出流的数据必须单独取出来
    val lowDStream: DataStream[(Int,Double,Long)]= allDStream.getSideOutput(new OutputTag[(Int,Double,Long)]("low"))
    lowDStream.print("侧输出流")
    /**
      *allDStream里虽然有两条流，但是我们对allDStream操作时操作的都是主流里的数据，也可以说默认就是主流里的数据，并不是全部流的数据。
      *要想使用侧输出流的数据必须单独用getSideOutput方法取出。可以说侧输出流就像隐藏起来了一样，我们不特别取出来用的都是主流里的数据。
      */
    env.execute("side_output_test")
  }
}

//ProcessFunction<I, O>   I是输入数据类型，O是主流的输出类型。因为是DataStream调用的process方法，所以没有key类型。
class MySplitTempProcessor(threshold: Double) extends ProcessFunction[SensorReading4,SensorReading4]{
  override def processElement(value: SensorReading4, ctx: ProcessFunction[SensorReading4, SensorReading4]#Context, out: Collector[SensorReading4]) = {
    //如果当前数据温度值大于30，那么输出到主流
    if(value.temperature > threshold){
        out.collect(value)    //collect方法输出到主流里。
    }else{ //output方法输出到侧输出流里
        // output(OutputTag<X> outputTag, X value)  outputTag是侧输出流标签，value是输出数据（格式类型要和outputTag里定义的一致）
        ctx.output(new OutputTag[(Int,Double,Long)]("low"),(value.id,value.temperature,value.ts))
    }
    //可见主流和侧输出流里的数据类型可以不一致，这也是使用 process方法完成分流比split方法的好处
  }
}
