package table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment

object Demo2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //这种默认创建的就是 老版本 planner的流式查询
    val table = StreamTableEnvironment.create(env)

    // 1.2老版本 planner的流式查询定义
    val settings = EnvironmentSettings.newInstance()
        .useOldPlanner()  //老版本
        .inStreamingMode() //流处理模式
        .build()

    val oldStreamTableEnv = StreamTableEnvironment.create(env,settings)

    // 1.3Blink的流式查询定义
    val bsSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()    //Blink
      .inStreamingMode()  //流处理模式
      .build()

    val bsTablEnv = StreamTableEnvironment.create(env,bsSettings)

    env.execute("demo2")
  }
}
