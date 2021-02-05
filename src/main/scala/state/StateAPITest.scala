package state

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state._

object StateAPITest {
  def main(args: Array[String]): Unit = {




  }
}

//要想在算子中使用状态，需要使用RichFunction，因为需要使用到运行时上下文
class MyRichMapper extends RichMapFunction[SensorReading,Int]{
  //单个值状态
  lazy val valueState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("value_state",classOf[Double]))//初始值为0.0
  //list类型结构的状态
  lazy val listState: ListState[Double] = getRuntimeContext.getListState(new ListStateDescriptor[Double]("list_state",classOf[Double]))//初始值为一个空list
  //map类型结构状态
  lazy val mapState: MapState[Int, Double] = getRuntimeContext.getMapState(new MapStateDescriptor[Int,Double]("map_state",classOf[Int],classOf[Double]))

  override def map(in: SensorReading) = {
    //读取状态里的值
    val value = valueState.value()
    //修改状态里的值
    valueState.update(in.temperature)
    valueState.clear()  //清空状态

    //list里增加值
    listState.add(in.temperature)
    val list = new java.util.ArrayList[Double]()
    list.add(1.0)
    list.add(2.0)
    listState.addAll(list)  //将一个java list里所有数据添加到list状态中.
    listState.update(list)  //用指定的list替换掉list状态
    val iter = listState.get()//获取list状态里所有的值
    listState.clear() //清空所有状态

    mapState.contains(1)//判断是否包含指定的key
    mapState.get(1) //获取指定key对应的value
    mapState.put(1,30.0)  //添加或者更新指定的kv
    val iterKey = mapState.keys() //获取所有key
    val iterValue = mapState.values() //获取所有value
    mapState.remove(1)  //删除指定的kv
    val kvIter = mapState.iterator() //遍历所有kv
    mapState.clear()  //清空所有状态

    in.id
  }
}