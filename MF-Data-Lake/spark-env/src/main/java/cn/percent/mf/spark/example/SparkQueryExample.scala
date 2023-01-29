package cn.percent.mf.spark.example

import org.apache.hudi.DataSourceReadOptions
import org.apache.spark.sql.SparkSession

/**
 * @author: wangshengbin
 * @date: 2022/4/29 5:14 PM
 */
object SparkQueryExample {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local[4]").appName("query-hudi")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    session.sparkContext.setLogLevel("Error")
    // write
    // val df = session.read.json("file:////Users/wangshengbin/shengbin/java-mediaforce-v4/MF-Data-Lake/spark-env/src/main/java/cn/percent/mf/spark/data/jsondata.json")
    // 更新数据
//    val df = session.read.json("file:////Users/wangshengbin/shengbin/java-mediaforce-v4/MF-Data-Lake/spark-env/src/main/java/cn/percent/mf/spark/data/updatedata.json")
//    df.write.format("hudi")
//      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key(),"id")
//      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key(),"data_dt")
//      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key(),"loc")
//      .option("hoodie.upsert.shuffle.parallelism","2")
//      .option("hoodie.insert.shuffle.parallelism","2")
//      .option(HoodieWriteConfig.TBL_NAME.key(),"person_infos2")
//      .mode(SaveMode.Append)
//      .save("/hudi_data/person_infos2")


    // read from hudi
//    val result = session.read.format("hudi")
//      .option(DataSourceReadOptions.QUERY_TYPE.key(), DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
//      .option(DataSourceReadOptions.QUERY_TYPE.key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
//      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key(), "20210429173359227")
//      .option(DataSourceReadOptions.END_INSTANTTIME.key(), "20220429173359227")
//      .load("/hudi_data/person_infos2/*/*")
//    result.show()

    val result = session.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key(), DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
      // .option(DataSourceReadOptions.QUERY_TYPE.key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      // .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key(), "20220628141928468")
      // .option(DataSourceReadOptions.END_INSTANTTIME.key(), "20220628151928468")
       .load("/hudi_data/person_infos/*")
      //.load("/shengbin/company/*/*")
    result.createOrReplaceTempView("t2")
    val frame = session.sql("select * from t2")
    frame.show(false)
  }
}
