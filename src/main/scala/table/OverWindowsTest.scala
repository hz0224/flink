package table

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row


object OverWindowsTest {
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
    //查询
    val resultTable = tableEnv.sqlQuery(
      """
        |select
        |    id,
        |    name,
        |    age,
        |    class_name,
        |    ts,
        |    count(1) over (partition by class_name order by et) as cnt, -- 本条数据所属窗口内有多少条数据
        |    sum(age) over (partition by class_name order by et) as avg_age -- 本条数据所属窗口内数据的平均年龄
        |from
        |    stu
      """.stripMargin)

    resultTable.toAppendStream[Row].print()

    env.execute("over_windows_test")
  }

}
