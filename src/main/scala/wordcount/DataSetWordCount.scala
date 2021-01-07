package wordcount

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

//批处理 wordCount
object DataSetWordCount {
  def main(args: Array[String]): Unit = {
    //创建批处理的执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //读取文件
    val inputDS: DataSet[String] = env.readTextFile("C:\\Users\\lenovo\\Desktop\\test.txt")

    val word2OneDS: DataSet[(String,Int)] = inputDS.flatMap(_.split(" ")).map((_,1))

    //在flink中没有 reduceByKey这样的算子，它将这样的算子分成了两部分,即 分组和计算.
    //按照元组中的第一个字段分组，然后每组按照第二个字段求和.
    val result: DataSet[(String,Int)] = word2OneDS
                                    .groupBy(0)   //groupBy(fields: Int*),以二元组中第一个元素作为key进行分组
                                    .sum(1)       //对分完组后的数据按照第二个元素进行sum
    result.print()
  }
}
