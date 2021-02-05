package state

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object StateBackendsTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //MemoryStateBackend
    env.setStateBackend(new MemoryStateBackend())
    //FsStateBackend
    env.setStateBackend(new FsStateBackend("hdfs_path or file:///tmp/checkpoints"))
    //RocksDBStateBackend，需要单独引包
    env.setStateBackend(new RocksDBStateBackend("path"))
    env.enableCheckpointing(5 * 60 * 1000L)
    // 配置重启策略
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(99,
      org.apache.flink.api.common.time.Time.of(5, TimeUnit.MINUTES)))

    env.execute("state_backends_test")
  }

}
