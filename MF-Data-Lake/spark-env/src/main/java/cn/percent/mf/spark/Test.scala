package cn.percent.mf.spark

import org.apache.spark.sql.SparkSession

/**
 * @author: wangshengbin
 * @date: 2022/4/14 8:51 PM
 */
object Test {
  def main(args: Array[String]): Unit = {
    queryData()
  }
  def queryData(): Unit = {
    System.setProperty("HADOOP_USER_NAME", "platform")
    val tableName = "mf_data"
    // 1. 构建Spark运行环境
    val spark = SparkSession
      .builder()
      .master("spark://bfd-wfj-67p123:7077")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.default.parallelism", 4)
      .appName("Load order data")
      .getOrCreate()
    val basePath = "hdfs://bfd-wfj-67p121:8020/hudi/warehouse"

    val mfDataDF = spark.
      read.
      format("hudi").
      load(basePath + "/*")

    mfDataDF.createOrReplaceTempView("hudi_mf_data_snapshot");

    // 查看数据
    // spark.sql("select distinct(_hoodie_commit_time) as commitTime from  hudi_trips_snapshot order by commitTime limit 1").map(k=>k.getString(0)).show()
    spark
      .sql("elect distinct(_hoodie_commit_time) as commitTime from  hudi_trips_snapshot order by commitTime limit 1")
      .show()
  }
}
