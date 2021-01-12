package split_stream

import aggr.Stu
import org.apache.flink.streaming.api.scala._

object unionTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputDStream = env.readTextFile("C:\\Users\\Administrator\\Desktop\\stu.txt")
    val stuDStream = inputDStream.map{line=>
      val data = line.split(",")
      Stu(data(0).toInt, data(1), data(2).toInt, data(3))
    }

    val splitDStream: SplitStream[Stu] = stuDStream.split{stu=>
      if (stu.age >= 18) Seq("adult") else Seq("nonage")
    }

    val adultDStream: DataStream[Stu] = splitDStream.select("adult")
    val nonageDStream: DataStream[Stu] = splitDStream.select("nonage")

    //使用union合流，要求多个流的数据类型一样
    val allDStream = adultDStream.union(nonageDStream)

    allDStream.print()
    env.execute("split_stream_test")
  }

}
