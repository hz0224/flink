package table

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object GroupWindowsTest {
  def main(args: Array[String]): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

      val tableEnv = StreamTableEnvironment.create(env)
      val inputDStream = env.readTextFile("C:\\Users\\Administrator\\Desktop\\stu.txt")
      val tupleDStream = inputDStream.map{line=>
        val data = line.split(",")
        (data(0).toInt, data(1), data(2).toInt, data(3),data(4).toLong)
      }.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(Int, String, Int,String, Long)](Time.seconds(0L)) {
        override def extractTimestamp(t: (Int, String, Int, String,Long)): Long = t._5 * 1000
      })
      val table = tableEnv.fromDataStream(tupleDStream,'id, 'name, 'age, 'class_name,'ts, 'et.rowtime)

      //注册表
      tableEnv.createTemporaryView("stu",table)
      val resultTable = tableEnv.sqlQuery(
        """
          |select
          |    class_name,
          |    count(1),  -- 这个窗口有多少条数据
          |    sum(age),
          |    tumble_start(et, interval '5' second), -- tumble_start函数 这个窗口的开始时间
          |    tumble_end(et, interval '5' second)  -- tumble_end函数 这个窗口的结束时间
          |from
          |    stu
          |group by
          |    class_name,tumble(et, interval '5' second) -- 5秒的滚动窗口
        """.stripMargin)

      //这里为什么用toAppendStream方法呢，明明count是会不断修改值的。
      //是因为这里使用了窗口，等到窗口关闭时才会进行一次统一计算，是计算一次得到结果的，并不是来一条计算一条的。
      resultTable.toAppendStream[Row].print()

      env.execute("group_windows_test")
  }
}
