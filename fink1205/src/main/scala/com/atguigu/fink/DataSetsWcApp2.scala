package com.atguigu.fink

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

object DataSetsWcApp2 {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val dataSet: DataSet[String] = environment.readTextFile("D:\\bigdata\\flink\\fink1205\\src\\main\\scala\\com\\atguigu\\fink\\wc.txt")
    val aggSet: AggregateDataSet[(String, Int)] = dataSet.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)
    aggSet.print()
  }
}
