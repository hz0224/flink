package transform
import org.apache.flink.streaming.api.scala._

object MapTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputDStream: DataStream[String] = env.fromCollection(List("I love Beijing","I love China"))
    inputDStream.print()

    //map算子，一对一转换
    val arrayDStream: DataStream[Array[String]] =  inputDStream.map{line=>
      line.split(" ")
    }
    arrayDStream.print()

    //flatMap，一对多转换，先执行map算子然后进行扁平化操作
    val flatDStream: DataStream[String]  =  arrayDStream.flatMap{array=>
      array
    }
    flatDStream.print()

    //filter算子，进行过滤操作，返回true的会保留，返回false会被过滤掉
    val filterDStream = flatDStream.filter{e=>
      "love".equals(e)
    }
    filterDStream.print()

    env.execute("map_test")

  }
}
