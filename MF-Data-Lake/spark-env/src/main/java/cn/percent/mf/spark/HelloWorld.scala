//package cn.percent.mf.spark
//
//import org.apache.spark.{SparkConf, SparkContext}
//
//
///**
// * @author: wangshengbin
// * @date: 2022/4/14 7:33 PM
// */
//object HelloWorld {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setMaster("local").setAppName("HelloWorld")
//
//    val sc = new SparkContext(conf)
//
//    val helloWorld = sc.parallelize(List("Hello,World!", "Hello,Spark!", "Hello,BigData!"))
//
//    helloWorld.foreach(line => println(line))
//  }
//}
