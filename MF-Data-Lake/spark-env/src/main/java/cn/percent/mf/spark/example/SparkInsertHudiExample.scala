package cn.percent.mf.spark.example

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.sql.functions.{col, concat_ws}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * @author: wangshengbin
 * @date: 2022/4/29 2:00 PM
 */
object SparkInsertHudiExample {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val session: SparkSession = SparkSession.builder().master("local").appName("test")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    session.sparkContext.setLogLevel("Error")

    val df = session.read.json("file:////Users/wangshengbin/shengbin/java-mediaforce-v4/MF-Data-Lake/spark-env/src/main/java/cn/percent/mf/spark/data/jsondata.json")
    val newdf: DataFrame = df.withColumn("partition_key", concat_ws("-", col("data_dt"), col("loc")))

    newdf.show()

    newdf.write.format("hudi")
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key(), "id")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key(), "data_dt")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key(), "partition_key")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option(HoodieWriteConfig.TBL_NAME.key(), "person_infos")
      .mode(SaveMode.Append)
      // 关于Hive设置,指定HiveServer2 连接url
      // .option(DataSourceWriteOptions.HIVE_URL.key(), "jdbc:hive2://bjwfj-67p77-mediaforce-24:10000")
      .save("/hudi_data/person_infos")

  }
}
