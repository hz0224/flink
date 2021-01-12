package aggr

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._
case class Stu1(id: Int, name: String, age: Int, class_name: String)

object ReduceTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputDStream = env.readTextFile("C:\\Users\\Administrator\\Desktop\\stu.txt")

    //同一个班级里，求学号的最大值，名字的最大值，年龄的最小值. 封装成一个 Stu1对象返回.
    val stuDStream = inputDStream.map{line=>
      val data = line.split(",")
      Stu1(data(0).toInt, data(1), data(2).toInt, data(3))
    }

    //reduce操作要求传入什么类型返回也要是这个类型。
    val resultDStream = stuDStream.keyBy(_.class_name)
       // .reduce(new MyReduceFunction)
                                  .reduce{(result: Stu1,next: Stu1)=>
                                            val name = if (result.name.compareTo(next.name) > 0) result.name else next.name
                                            Stu1(result.id.max(next.id), name , result.age.min(next.age), result.class_name)
                                  }
    //可见,reduce操作可以灵活的自定义聚合方式，自定义怎样聚合前一条数据和后一条数据得到一个最新的数据。就是一个归约的过程。

    resultDStream.print()
    env.execute("reduce_test")
  }
}


//还可以使用ReduceFunction类，实现里面的reduce方法,泛型就是要处理的类型.也就是调用reduce方法的那个DataStream里面的泛型
class MyReduceFunction extends ReduceFunction[Stu1]{
  override def reduce(result: Stu1, next: Stu1) = {
    val name = if (result.name.compareTo(next.name) > 0) result.name else next.name
    Stu1(result.id.max(next.id), name , result.age.min(next.age), result.class_name)
  }
}
