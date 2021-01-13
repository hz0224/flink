package window

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object WindowAPITest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    val inputDStream = env.socketTextStream("39.105.49.35",9999)
    val resultDStream = inputDStream.flatMap(_.split(" ")).map((_,1))
                  .keyBy(_._1)
                  //.window(TumblingEventTimeWindows.of(Time.seconds(5),Time.minutes(5)))       //滚动时间窗口,第二个参数是偏移量
                  //.window(SlidingEventTimeWindows.of(Time.seconds(15), Time.seconds(5), Time.minutes(5)))  //滑动时间窗口，第三个参数代表偏移量.
                  //.countWindow(10,5)  //计数窗口，传一个参数代表滚动，传两个参数代表滑动
                  //.window(EventTimeSessionWindows.withGap(Time.seconds(10)))  //会话窗口
                  .timeWindow(Time.seconds(5))  // timeWindow是window方法的简写形式，传一个参数代表是滚动窗口，传两个参数代表是滑动窗口.无法传入偏移量
                 // 如果要使用偏移量，还是需要使用底层的window方法。
                  .sum(1)

    resultDStream.print()
    /**
      * 1 window方法是底层的方法，是关于时间窗口的，是keyedStream里的方法，因此必须在使用了keyBy之后调用，里面需要传入一个窗口分配器WindowAssigner
      * 2 没有开窗之前数据是无界数据流，使用window方法后相当于变成了有界数据流，只是由 keyedStream ---> WindowedStream，就把它当成是批处理就好了。
      * 3 调用window方法后并没有结束，因为还没有定义如何对窗口里的数据进行计算，还必须再调用聚合方法进行计算。
      * 4 使用了window后调用聚合方法时和使用keyBy之后调用聚合方法计算逻辑是一样的，调用聚合函数时每个分区的每个窗口按照key进行分组，每个分组的数据到来时都会调用一次聚合函数。
      * 5 DataStream可以使用 windowAll进行开窗，传入的也是一个window分配器，返回一个AllWindowedStream。但由于没有分区，因此效率较低，不建议使用。
      * 6 countWindow的底层用到了 全局窗口GlobalWindows，全局窗口会把所有的数据丢到一个窗口里，需要自定义触发器来触发这个窗口的计算。
      */
    env.execute("window_API_test")
  }

}
