package com.atguigu.fink

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

object DataSetsWcApp {
  //批处理wordcount
  def main(args: Array[String]): Unit = {
    print("hello world\n")
    //1.构建环境 environment 2.读取文件source   3.transfer  4 sink
    val env = ExecutionEnvironment.getExecutionEnvironment
    val dateSet: DataSet[String] = env.readTextFile("D:\\bigdata\\flink\\fink1205\\src\\main\\scala\\com\\atguigu\\fink\\wc.txt")
    val aggSet: AggregateDataSet[(String, Int)] = dateSet.flatMap((_: String).split(" ")).map((_, 1)).groupBy(0).sum(1)
    aggSet.print()


  }

}
