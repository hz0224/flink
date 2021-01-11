package source

import org.apache.flink.streaming.api.scala._
case class Stu(id: Int,name: String,age: Int)

//从集合和文件里读取数据
object FileSource {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stulist = List(
      Stu(1,"jack",20),
      Stu(2,"mack",22),
      Stu(3,"lele",18)
    )

    //从集合里读取数据
    val inputDStream: DataStream[Stu] = env.fromCollection(stulist) //参数是Seq[T]或者Iterator[T]类型
    //env.fromElements("jack","lala","jerry")  //fromElements方法可以接受任何类型
    inputDStream.print()

    //从文件里读取数据
    val fileDStream: DataStream[String] = env.readTextFile("C:\\Users\\lenovo\\Desktop\\stu.txt")
    fileDStream.print()

    //虽然使用的是实时API,但从集合或者文件里读取的还是一个有界流，数据读取计算完程序也就自动结束了.
    env.execute("file_source")
  }

}
