package split_stream

import aggr.Stu
import org.apache.flink.streaming.api.scala._

object SplitStreamTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputDStream = env.readTextFile("C:\\Users\\Administrator\\Desktop\\stu.txt")
    val stuDStream = inputDStream.map{line=>
      val data = line.split(",")
      Stu(data(0).toInt, data(1), data(2).toInt, data(3))
    }

    //调用split方法后会返回一个 SplitStream，里面包含多个DataStream流，splitDStream里有所有流的数据
    val splitDStream: SplitStream[Stu] = stuDStream.split{stu=>
      if (stu.age >= 18) Seq("adult") else Seq("nonage")
      //使用Seq或List,Set按照条件对流进行打标记，并不是只能返回两个流，可以返回多个，代表多个流.
    }

    //调用select方法根据标记选需要的流即可
    val adultDStream: DataStream[Stu] = splitDStream.select("adult")
    //select方法也可以同时选择多个流
    val allDStream: DataStream[Stu] = splitDStream.select("adult","nonage")


    adultDStream.print("adultDStream")    //print方法在打印流时可以传入一个字符串代表当前打印的是哪条流
    allDStream.print("allDStream")

    env.execute("split_stream_test")
  }

}
