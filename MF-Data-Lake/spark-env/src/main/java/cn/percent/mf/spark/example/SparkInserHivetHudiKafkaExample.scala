package cn.percent.mf.spark.example

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.spark.sql.functions.{col, concat_ws}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * @author: wangshengbin
 * @date: 2022/4/29 2:00 PM
 */
object SparkInserHivetHudiKafkaExample {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val session: SparkSession = SparkSession.builder().master("local").appName("test")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    session.sparkContext.setLogLevel("Error")


    val df = session.read.json("file:////Users/wangshengbin/shengbin/java-mediaforce-v4/MF-Data-Lake/spark-env/src/main/java/cn/percent/mf/spark/data/jsondata.json")
    val newdf: DataFrame = df.withColumn("partition_key", concat_ws("-", col("data_dt"), col("loc")))

    newdf.show()

    df.write.format("hudi")
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key(), "docId")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key(), "createTime")
      //指定分区列
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key(), "pubDay")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option(HoodieWriteConfig.TBL_NAME.key(), "person_infos")
      .option("hoodie.datasource.write.keygenerator.class","org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator")
      .option("hoodie.deltastreamer.keygen.timebased.timestamp.type","EPOCHMILLISECONDS")
      .option("hoodie.deltastreamer.keygen.timebased.output.dateformat","yyyy-MM-dd")
      .mode(SaveMode.Append)
      // 关于Hive设置,指定HiveServer2 连接url
      .option(DataSourceWriteOptions.HIVE_URL.key(), "jdbc:hive2://bjwfj-67p77-mediaforce-24.bfdabc.com:10000")
      // 指定Hive 对应的库名
      .option(DataSourceWriteOptions.HIVE_DATABASE.key(), "default")
      // 指定Hive映射的表名称
      .option(DataSourceWriteOptions.HIVE_TABLE.key(), "mf_company_1686_snap")
      // Hive表映射对的分区字段
      .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS.key(), "pubDay")
      .option(DataSourceWriteOptions.HIVE_USER.key(), "hdfs")
      // 当设置为true时，注册/同步表到Apache Hive metastore,默认是false，这里就是自动创建表
      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED.key(), "true")
      // 如果分区格式不是yyyy/mm/dd ，需要指定解析类将分区列解析到Hive中
      .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS.key(), classOf[MultiPartKeysValueExtractor].getName)
      .save("/shengbin/company2")

  }
}
