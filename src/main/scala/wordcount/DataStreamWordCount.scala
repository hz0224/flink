package wordcount

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

//流处理 wordCount
object DataStreamWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //设置并行度(几个线程)，单核也可以多线程，只不过是CPU时间片快速切换，是假的多线程。
    //要想实现真正的多线程并行，还是需要核的支持。会基于key的哈希值分配线程(分配分区)但不完全依赖key的哈希值分区。
    //不同的key可能会到同一个线程，但相同的key一定会在同一个线程.
    //在flink中每一个算子后面都可以调用 setParallelism方法为该算子去设置并行度，这意味着前后两个算子的并行度是可以不同的.
    //比如执行map算子时是3个并行度，执行sum算子时可以是2个并行度。
    //有时候在进行打印输出时，为了防止多线程同时操作顺序混乱，我们会把并行度设置为1


    //当需要从外部命令获取参数时，除了直接使用args外还可以使用flink提供好的工具来提取参数
    val tool = ParameterTool.fromArgs(args)
    val host: String = tool.get("host")
    val port: Int = tool.getInt("port")

    val inputStream: DataStream[String] = env.socketTextStream("39.105.49.35", 9999)

    val wordStream: DataStream[String] = inputStream.flatMap { line => line.split(" ") }

    val word2OneStream: DataStream[(String,Int)] = wordStream.map { word => (word, 1) }

    //[(word,one),JavaTuple]   JavaTuple是java里的类型，里面就是你keyBy(0)对应的那个数据类型，这里是(word)String.
    //但这里使用一个JavaTuple明显是多余的，究其根本还是因为使用了 keyBy(0)这种下标的方式去代表按照哪个字段分区
    //因为scala无法根据下标推断出这个下标所代表的类型，自然也就无法推断出JavaTuple的类型
    //val keyByStream: KeyedStream[(String, Int), Tuple] = word2OneStream.keyBy(0)


    //[(word,one),word]     KeyedStream是一个有状态的流，DataStream是没有状态的。
    val keyByStream: KeyedStream[(String, Int), String] = word2OneStream.keyBy(_._1)
    //方法源码：def keyBy[K: TypeInformation](fun: T => K): KeyedStream[T, K]
    //方法的参数是一个函数f，f的形参是T类型，返回是K类型。keyBy方法返回[T,K]类型，可见使用过keyBy算子后生成的
    //Stream和上一个Stream的类型保持一致，都是T类型。

    keyByStream.print()

    //使用过聚合函数后，KeyedStream类型又变成了无状态的DataStream类型,并且里面的类型和之前一样。
    val word2Count: DataStream[(String,Int)] = keyByStream.sum(1)

    word2Count.print()

    env.execute("data_stream_word_count")
    //需要反复执行，等待事件去驱动，每来一条数据就会触发一次计算同时打印出这条数据计算的结果，没有计算的数据不会打印出来。
    //这也是和批处理的不同之处。
  }
}
