package cn.percent.mf.data.lake.example

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * @author: wangshengbin
 * @date: 2022/5/7 4:27 PM
 */
object SqlScalaKafkaFlink {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    // 创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(2000)
    // table sql
    val tableEnv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build())
    // kafka source table
    tableEnv.executeSql(
      """
        | create table k1 (
        |   docId STRING,
        |   age INT,
        |   area STRING,
        |   attitudesCount INT,
        |   author STRING,
        |   avatar STRING,
        |   biz STRING,
        |   btTaskId STRING,
        |   bulletCount INT,
        |   channel STRING,
        |   city STRING,
        |   coinsCount INT,
        |   collectionCount INT,
        |   content STRING,
        |   contentHtml STRING,
        |   contentSimHash STRING,
        |   country STRING,
        |   crawlDate STRING,
        |   crawlDay BIGINT,
        |   crawlTime BIGINT,
        |   crawlTimeStr STRING,
        |   createDate STRING,
        |   createDay BIGINT,
        |   createTime BIGINT,
        |   createTimeStr STRING,
        |   dataSourceType STRING,
        |   docType STRING,
        |   fansCount STRING,
        |   finalPhrase STRING,
        |   forwardAuthor STRING,
        |   forwardContent STRING,
        |   forwardFlag INT,
        |   forwardPics STRING,
        |   forwardPictureList STRING,
        |   forwardReferUrl STRING,
        |   forwardReleaseDate STRING,
        |   forwardUid STRING,
        |   forwardUrl STRING,
        |   forwardUserUrl STRING,
        |   friendsCount INT,
        |   hlAbstract STRING,
        |   hlKeywords STRING,
        |   hyTaskType STRING,
        |   hylandaDocId STRING,
        |   keyword STRING,
        |   kid STRING,
        |   likeCount INT,
        |   location STRING,
        |   manualSentiment STRING,
        |   nodupUrlKey STRING,
        |   ocr ARRAY<ROW<mfUrl STRING,content STRING,url STRING>>,
        |   originDocId STRING,
        |   originaUrl STRING,
        |   originalContentSimHash STRING,
        |   originalFrom STRING,
        |   originalPhrase STRING,
        |   originalSource STRING,
        |   originalTitleSimHash STRING,
        |   originalUrl STRING,
        |   picUrls STRING,
        |   pictureList STRING,
        |   postSource STRING,
        |   post_id STRING,
        |   postsCount STRING,
        |   province STRING,
        |   pubDate STRING,
        |   pubDay BIGINT,
        |   pubTime BIGINT,
        |   pubTimeStr STRING,
        |   quoteCount INT,
        |   relativity STRING,
        |   relavancy STRING,
        |   repeatCount INT,
        |   replyCount INT,
        |   repostCount INT,
        |   selfMedia STRING,
        |   sex STRING,
        |   shareCount INT,
        |   siteDomain STRING,
        |   siteName STRING,
        |   source STRING,
        |   sourceUrl STRING,
        |   spamTag STRING,
        |   subjectKeyName STRING,
        |   subjectRawName STRING,
        |   subjectType STRING,
        |   sysAbstract STRING,
        |   sysKeywords STRING,
        |   sysSentiment DOUBLE,
        |   sysSentimentTag STRING,
        |   textArea STRING,
        |   textProvince STRING,
        |   title STRING,
        |   titleLength STRING,
        |   titleSimHash STRING,
        |   url STRING,
        |   urlHasg STRING,
        |   userId STRING,
        |   userUrl STRING,
        |   verified STRING,
        |   videoImgUrl STRING,
        |   videoPicList STRING,
        |   videoUrlList STRING,
        |   videoUrls ARRAY<STRING>,
        |   visitCount STRING,
        |   wechatCode STRING,
        |   weiboId STRING,
        |   weiboMid STRING,
        |   weixinName STRING,
        |   zaikanCount STRING,
        |   zhxgChannel STRING,
        |   zhxgDataType STRING,
        |   zhxgKeyWords STRING,
        |   zhxgSubjectId STRING,
        |   zhxgSubjectName STRING
        | ) with(
        |     'connector' = 'kafka',
        |     'topic' = 'mf.streaming.data.realtime.v1',
        |     'properties.bootstrap.servers' = '192.168.67.124:9092,192.168.67.125:9092,192.168.67.137:9092',
        |     'properties.group.id' = 'test-flink-con',
        |     'scan.startup.mode' = 'group-offsets',
        |     'format' = 'json',
        |     'json.fail-on-missing-field' = 'false',
        |     'json.ignore-parse-errors' = 'true'
        | )
        |""".stripMargin
    )
    val table = tableEnv.from("k1")
    // 测试查询
    //    tableEnv.executeSql(
    //      s"""
    //        | select * from ${table} limit 1
    //        |""".stripMargin
    //    ).print()

    // to hudi
    tableEnv.executeSql(
      """
        | create table if not exists h1 (
        |   docId STRING PRIMARY KEY  NOT ENFORCED,
        |   age INT,
        |   area STRING,
        |   attitudesCount INT,
        |   author STRING,
        |   avatar STRING,
        |   biz STRING,
        |   btTaskId STRING,
        |   bulletCount INT,
        |   channel STRING,
        |   city STRING,
        |   coinsCount INT,
        |   collectionCount INT,
        |   content STRING,
        |   contentHtml STRING,
        |   contentSimHash STRING,
        |   country STRING,
        |   crawlDate STRING,
        |   crawlDay BIGINT,
        |   crawlTime BIGINT,
        |   crawlTimeStr STRING,
        |   createDate STRING,
        |   createDay BIGINT,
        |   createTime BIGINT,
        |   createTimeStr STRING,
        |   dataSourceType STRING,
        |   docType STRING,
        |   fansCount STRING,
        |   finalPhrase STRING,
        |   forwardAuthor STRING,
        |   forwardContent STRING,
        |   forwardFlag INT,
        |   forwardPics STRING,
        |   forwardPictureList STRING,
        |   forwardReferUrl STRING,
        |   forwardReleaseDate STRING,
        |   forwardUid STRING,
        |   forwardUrl STRING,
        |   forwardUserUrl STRING,
        |   friendsCount INT,
        |   hlAbstract STRING,
        |   hlKeywords STRING,
        |   hyTaskType STRING,
        |   hylandaDocId STRING,
        |   keyword STRING,
        |   kid STRING,
        |   likeCount INT,
        |   location STRING,
        |   manualSentiment STRING,
        |   nodupUrlKey STRING,
        |   ocr ARRAY<ROW<mfUrl STRING,content STRING,url STRING>>,
        |   originDocId STRING,
        |   originaUrl STRING,
        |   originalContentSimHash STRING,
        |   originalFrom STRING,
        |   originalPhrase STRING,
        |   originalSource STRING,
        |   originalTitleSimHash STRING,
        |   originalUrl STRING,
        |   picUrls STRING,
        |   pictureList STRING,
        |   postSource STRING,
        |   post_id STRING,
        |   postsCount STRING,
        |   province STRING,
        |   pubDate STRING,
        |   pubDay BIGINT,
        |   pubTime BIGINT,
        |   pubTimeStr STRING,
        |   quoteCount INT,
        |   relativity STRING,
        |   relavancy STRING,
        |   repeatCount INT,
        |   replyCount INT,
        |   repostCount INT,
        |   selfMedia STRING,
        |   sex STRING,
        |   shareCount INT,
        |   siteDomain STRING,
        |   siteName STRING,
        |   source STRING,
        |   sourceUrl STRING,
        |   spamTag STRING,
        |   subjectKeyName STRING,
        |   subjectRawName STRING,
        |   subjectType STRING,
        |   sysAbstract STRING,
        |   sysKeywords STRING,
        |   sysSentiment DOUBLE,
        |   sysSentimentTag STRING,
        |   textArea STRING,
        |   textProvince STRING,
        |   title STRING,
        |   titleLength STRING,
        |   titleSimHash STRING,
        |   url STRING,
        |   urlHasg STRING,
        |   userId STRING,
        |   userUrl STRING,
        |   verified STRING,
        |   videoImgUrl STRING,
        |   videoPicList STRING,
        |   videoUrlList STRING,
        |   videoUrls ARRAY<STRING>,
        |   visitCount STRING,
        |   wechatCode STRING,
        |   weiboId STRING,
        |   weiboMid STRING,
        |   weixinName STRING,
        |   zaikanCount STRING,
        |   zhxgChannel STRING,
        |   zhxgDataType STRING,
        |   zhxgKeyWords STRING,
        |   zhxgSubjectId STRING,
        |   zhxgSubjectName STRING
        | ) PARTITIONED BY (`pubDay`)
        |  with(
        |     'connector' = 'hudi',
        |     'path' = '/shengbin/hudi',
        |     'write.tasks' = '6',
        |     'write.bucket_assign.tasks' = '6',
        |     'compaction.tasks' = '4',
        |     'table.type' = 'MERGE_ON_READ',
        |     'write.bulk_insert.sort_by_partition' = 'true',  -- Whether to sort the inputs by partition path for bulk insert tasks, default true
        |     'compaction.trigger.strategy' = 'num_commits',
        |     'compaction.max_memory' = '100',
        |     'compaction.async.enabled' = 'true',
        |     'write.bulk_insert.shuffle_by_partition' = 'true', -- Whether to shuffle the inputs by partition path for bulk insert tasks, default true
        |     'write.parquet.block.size' = '120',
        |     'compaction.timeout.seconds' = '1200', -- Max timeout time in seconds for online compaction to rollback, default 20 minutes
        |     'write.sort.memory' = '128',
        |     'write.retry.times' = '3', -- Flag to indicate how many times streaming job should retry for a failed checkpoint batch. By default 3
        |     'read.tasks' = '4', -- Parallelism of tasks that do actual read, default is 4
        |     'write.parquet.max.file.size' = '120', -- Target size for parquet files produced by Hudi write phases. For DFS, this needs to be aligned with the underlying filesystem block size for optimal performance
        |     'compaction.schedule.enabled' = 'true', -- Schedule the compaction plan, enabled by default for MOR
        |     'write.batch.size' = '256', -- Batch buffer size in MB to flush data into the underneath filesystem, default 256MB
        |     'clean.async.enabled' = 'true',
        |     'clean.retain_commits' = '10',
        |     'hoodie.datasource.write.keygenerator.class' = 'org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator',
        |     'hoodie.deltastreamer.keygen.timebased.timestamp.type' = 'EPOCHMILLISECONDS',
        |     'hoodie.deltastreamer.keygen.timebased.output.dateformat' = 'yyyy-MM-dd',
        |     'hoodie.embed.timeline.server' = 'false',
        |     'hoodie.datasource.query.type' = 'snapshot'
        | )
        |""".stripMargin
    )
    // insert
    tableEnv.executeSql(
      s"""
         | insert into h1 select * from ${table}
         |""".stripMargin
    )
  }
}
