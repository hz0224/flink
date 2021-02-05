package checkpoint

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object CheckpointTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //checkpoint的目录
    env.setStateBackend(new FsStateBackend("hdfs://emr-cluster/flink/flink-checkpoints/"))
    /**
      * 注意：checkpoint不是每个任务拍个快照，而是所有任务不同时间拍个快照后的一个合照。
      * enableCheckpointing(interval : Long, mode: CheckpointingMode)
      * interval代表多久触发一次checkpoint，时间短的话消耗资源，时间长的话影响实时性。
      * mode代表 一致性级别
      *   CheckpointingMode.EXACTLY_ONCE 精确一次，默认也是这个，可以不写。
      *   CheckpointingMode.AT_LEAST_ONCE 至少一次，对准确性要求不高，对实时性要求很高的场景下适用。
      */
    //启用checkpoint ，因为flink默认情况下是不启用checkpoint的
    env.enableCheckpointing(60 * 1000L,CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointInterval(60 * 1000L) //也可以通过这种方式设置
    env.getCheckpointConfig.setCheckpointTimeout(3 * 60 * 1000L)
    //如果checkpoint耗时超过指定的时间就不再做这个checkpoint，防止checkpoint过长影响效率。
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)    //设置有多少个checkpoint可以同时做，默认是一个.
    //配置重启策略，配置失败重启尝试次数3次，每次重启之间的间隔为3分钟。
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3 * 60 * 1000L))
    //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,org.apache.flink.api.common.time.Time.of(3, TimeUnit.MINUTES)))  也可以这样写

    env.execute("checkpoint_test")
  }

}
