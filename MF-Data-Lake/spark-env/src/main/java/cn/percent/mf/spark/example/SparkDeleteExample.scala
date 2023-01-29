package cn.percent.mf.spark.example

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * @author: wangshengbin
 * @date: 2022/4/29 5:15 PM
 */
object SparkDeleteExample {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local").appName("delete-hudi-data")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    //    val read = session.read.json("file:///Users/wangshengbin/shengbin/java-mediaforce-v4/MF-Data-Lake/spark-env/src/main/java/cn/percent/mf/spark/data/deldata.json")
    //    read.write.format("hudi")
    //      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key(), "id")
    //      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key(), "data_dt")
    //      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key(), "loc")
    //      .option(HoodieWriteConfig.TBL_NAME.key(), "person_infos3")
    //      .option("hoodie.upsert.shuffle.parallelism", "2")
    //      .option("hoodie.insert.shuffle.parallelism", "2")
    //      .mode(SaveMode.Append)
    //      .save("/hudi_data/person_infos3")

    val read = session.read.json("file:///Users/wangshengbin/shengbin/java-mediaforce-v4/MF-Data-Lake/spark-env/src/main/java/cn/percent/mf/spark/data/deldata1.json")
    read.write.format("hudi")
      .option(DataSourceWriteOptions.TABLE_TYPE.key(),DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.OPERATION.key(),"insert_overwrite_table")
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key(), "id")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key(), "loc")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key(), "data_dt")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.delete.shuffle.parallelism", "2")
      // .option("hoodie.cleaner.commits.retained","1")
      // .option("hoodie.compact.inline",true)
      // .option("hoodie.cleaner.commits.retained",1)
      // .option("hoodie.compact.inline.max.delta.commits",1)
      .option(HoodieWriteConfig.TBL_NAME.key(), "person_infos3")
      .mode(SaveMode.Append)
      .save("/hudi_data/person_infos3")

    session.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key(),DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
      .load("/hudi_data/person_infos3/*/*")
      .createOrReplaceTempView("t2")

    session.sql("select * from t2 where id in(1,2)")
      .show()

  }
}
