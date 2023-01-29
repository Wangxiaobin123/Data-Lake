package cn.percent.mf.data.lake.example

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table}

/**
 * @author: wangshengbin
 * @date: 2022/4/29 7:21 PM
 */
object SqlScalaMysqlFlink {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    //1.创建对象
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance()
      .useBlinkPlanner().inStreamingMode().build())

    //2.必须开启checkpoint 默认有5个checkpoint后，hudi目录下才会有数据，不然只有一个.hoodie目录。
    env.enableCheckpointing(2000)
    //    env.setStateBackend(new RocksDBStateBackend("hdfs://mycluster/flinkstate"))

    //3.设置并行度
    env.setParallelism(1)

    //4.读取Kakfa 中的数据
    tableEnv.executeSql(
      """
        | create table t1(
        |  id int,
        |  name string
        | ) with (
        |  'connector' = 'jdbc',
        |  'url'='jdbc:mysql://172.18.9.71:3306/test?autoReconnect=true',
        |  'username' = 'bfd_ibg_mf',
        |  'password' = 'bfd_ibg_mf@168',
        |  'username' = 'bfd_ibg_mf',
        |  'table-name' = 't1'
        | )
      """.stripMargin)

    val table: Table = tableEnv.from("t1")

    //5.创建Flink 对应的hudi表
    tableEnv.executeSql(
      """
        |CREATE TABLE test (
        |  id int PRIMARY KEY NOT ENFORCED,--默认主键列为uuid,这里可以后面跟上“PRIMARY KEY NOT ENFORCED”指定为主键列
        |  name string
        |)
        |PARTITIONED BY (name)
        |WITH (
        |  'connector' = 'hudi',
        |  'path' = '/flink_hudi_data',
        |  'write.tasks' = '1',
        |  'compaction.tasks' = '1',
        |  'table.type' = 'COPY_ON_WRITE',
        |  'hive_sync.partition_extractor_class' = 'org.apache.hudi.hive.MultiPartKeysValueExtractor',
        |   'hive_sync.enable' = 'true',
        |   'hive_sync.partition_fields' = 'name',
        |   'hive_sync.table' = 'shengbin',
        |   'hive_sync.username' = 'hdfs',
        |   'hive_sync.db' = 'default',
        |   'hive_sync.metastore.uris' = 'thrift://bjwfj-67p76-mediaforce-23.bfdabc.com:9083',
        |   'hive_sync.jdbc_url' = 'jdbc:hive2://bjwfj-67p77-mediaforce-24.bfdabc.com:10000'
        |)
      """.stripMargin)

    //6.向表中插入数据
    tableEnv.executeSql(
      s"""
         | insert into test select * from ${table}
      """.stripMargin)
    // env.execute("test")
  }
}
