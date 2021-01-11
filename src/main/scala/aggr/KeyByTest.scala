package aggr

import org.apache.flink.streaming.api.scala._

case class Stu(id: Int, name: String, age: Int, class_name: String)

object KeyByTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    val inputDStream = env.readTextFile("C:\\Users\\lenovo\\Desktop\\stu.txt")
    val stuDStream =  inputDStream.map{line=>
      val data = line.split(",")
      Stu(data(0).toInt, data(1),data(2).toInt, data(3))
    }
    val resultDStream = stuDStream.keyBy(_.class_name)    //按照class_name进行分区，也可以传入字符串class_name
                .minBy("age")    //这里就需要写字符串age
    //min和minBy的区别：
    //minBy可以返回最小age对应的那个stu对象的完整信息，而min不会。因此在使用时，最好使用minBy. maxBy同理
    resultDStream.print()
    env.execute("keyBy_test")
  }
}
