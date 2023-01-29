package cn.percent.mf.data.lake.example

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * @author: wangshengbin
 * @date: 2022/5/7 4:27 PM
 */
object SqlScalaCompanyKafkaFlink {
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
        |                   docId STRING,
        |                   age INT,
        |                   barrageCount INT,
        |                   area STRING,
        |                   screen STRING,
        |                   attitudesCount INT,
        |                   author STRING,
        |                   avatar STRING,
        |                   channel STRING,
        |                   city STRING,
        |                   coinsCount INT,
        |                   reachedRate INT,
        |                   collectionCount INT,
        |                   content STRING,
        |                   contentHtml STRING,
        |                   contentSimHash STRING,
        |                   country STRING,
        |                   crawlTime BIGINT,
        |                   crawlTimeStr STRING,
        |                   createDate STRING,
        |                   createDay BIGINT,
        |                   createTime BIGINT,
        |                   createTimeStr STRING,
        |                   dataSourceType STRING,
        |                   docType STRING,
        |                   fansCount INT,
        |                   forwardAuthor STRING,
        |                   forwardContent STRING,
        |                   forwardFlag INT,
        |                   forwardReleaseDate STRING,
        |                   forwardUrl STRING,
        |                   friendsCount INT,
        |                   keyword STRING,
        |                   promulgator STRING,
        |                   likeCount INT,
        |                   location STRING,
        |                   ocr ARRAY<ROW<mfUrl STRING,content STRING,url STRING>>,
        |                   subjects ARRAY<ROW<id BIGINT,relativity STRING,emotion DOUBLE,label2 ARRAY<STRING>,label1 ARRAY<STRING>,forwardContentDigest STRING,titleDigest STRING,contentDigest STRING,matchContent INT,matchOcr INT,matchTitle INT,version INT>>,
        |                   originalUrl STRING,
        |                   picUrls STRING,
        |                   province STRING,
        |                   pubDate STRING,
        |                   pubDay BIGINT,
        |                   pubTime BIGINT,
        |                   pubTimeStr STRING,
        |                   quoteCount INT,
        |                   relativity STRING,
        |                   repeatCount INT,
        |                   commentsCount INT,
        |                   replyCount INT,
        |                   repostCount INT,
        |                   selfMedia STRING,
        |                   sex STRING,
        |                   shareCount INT,
        |                   interactionCount INT,
        |                   coinCount INT,
        |                   watchingCount INT,
        |                   collectCount INT,
        |                   siteDomain STRING,
        |                   siteName STRING,
        |                   source STRING,
        |                   sysAbstract STRING,
        |                   sysSentiment DOUBLE,
        |                   importance DOUBLE,
        |                   score DOUBLE,
        |                   propagationRate DOUBLE,
        |                   textArea STRING,
        |                   textProvince STRING,
        |                   title STRING,
        |                   titleSimHash STRING,
        |                   url STRING,
        |                   userId STRING,
        |                   userUrl STRING,
        |                   verified STRING,
        |                   videoImgUrl STRING,
        |                   videoPicList STRING,
        |                   videoUrlList STRING,
        |                   videoUrls ARRAY<STRING>,
        |                   wordCloud ARRAY<STRING>,
        |                   visitCount INT,
        |                   wechatCode STRING,
        |                   weiboId STRING,
        |                   zaikanCount STRING,
        |                   zhxgChannel STRING,
        |                   mediaClassify STRING
        | ) with(
        |     'connector' = 'kafka',
        |     'topic' = 'hudi-c-test',
        |     'properties.bootstrap.servers' = '192.168.67.124:9092,192.168.67.125:9092,192.168.67.137:9092',
        |     'properties.group.id' = 'test-c-sink-hudi',
        |     'scan.startup.mode' = 'earliest-offset',
        |     'format' = 'json',
        |     'json.fail-on-missing-field' = 'false',
        |     'json.ignore-parse-errors' = 'true'
        | )
        |""".stripMargin
    )
    val table = tableEnv.from("k1")
    // 测试查询
//        tableEnv.executeSql(
//          s"""
//            | select subjects from ${table} limit 1
//            |""".stripMargin
//        ).print()

    // to hudi
    tableEnv.executeSql(
      """
        | create table if not exists mf_company_1200_snap_hudi (
        |                   docId STRING PRIMARY KEY  NOT ENFORCED,
        |                   age INT,
        |                   barrageCount INT,
        |                   area STRING,
        |                   screen STRING,
        |                   attitudesCount INT,
        |                   author STRING,
        |                   avatar STRING,
        |                   channel STRING,
        |                   city STRING,
        |                   coinsCount INT,
        |                   reachedRate INT,
        |                   collectionCount INT,
        |                   content STRING,
        |                   contentHtml STRING,
        |                   contentSimHash STRING,
        |                   country STRING,
        |                   crawlTime BIGINT,
        |                   crawlTimeStr STRING,
        |                   createDate STRING,
        |                   createDay BIGINT,
        |                   createTime BIGINT,
        |                   createTimeStr STRING,
        |                   dataSourceType STRING,
        |                   docType STRING,
        |                   fansCount INT,
        |                   forwardAuthor STRING,
        |                   forwardContent STRING,
        |                   forwardFlag INT,
        |                   forwardReleaseDate STRING,
        |                   forwardUrl STRING,
        |                   friendsCount INT,
        |                   keyword STRING,
        |                   promulgator STRING,
        |                   likeCount INT,
        |                   location STRING,
        |                   ocr ARRAY<ROW<mfUrl STRING,content STRING,url STRING>>,
        |                   subjects ARRAY<ROW<id BIGINT,relativity STRING,emotion DOUBLE,label2 ARRAY<STRING>,label1 ARRAY<STRING>,forwardContentDigest STRING,titleDigest STRING,contentDigest STRING,matchContent INT,matchOcr INT,matchTitle INT,version INT>>,
        |                   originalUrl STRING,
        |                   picUrls STRING,
        |                   province STRING,
        |                   pubDate STRING,
        |                   pubDay BIGINT,
        |                   pubTime BIGINT,
        |                   pubTimeStr STRING,
        |                   quoteCount INT,
        |                   relativity STRING,
        |                   repeatCount INT,
        |                   commentsCount INT,
        |                   replyCount INT,
        |                   repostCount INT,
        |                   selfMedia STRING,
        |                   sex STRING,
        |                   shareCount INT,
        |                   interactionCount INT,
        |                   coinCount INT,
        |                   watchingCount INT,
        |                   collectCount INT,
        |                   siteDomain STRING,
        |                   siteName STRING,
        |                   source STRING,
        |                   sysAbstract STRING,
        |                   sysSentiment DOUBLE,
        |                   importance DOUBLE,
        |                   score DOUBLE,
        |                   propagationRate DOUBLE,
        |                   textArea STRING,
        |                   textProvince STRING,
        |                   title STRING,
        |                   titleSimHash STRING,
        |                   url STRING,
        |                   userId STRING,
        |                   userUrl STRING,
        |                   verified STRING,
        |                   videoImgUrl STRING,
        |                   videoPicList STRING,
        |                   videoUrlList STRING,
        |                   videoUrls ARRAY<STRING>,
        |                   wordCloud ARRAY<STRING>,
        |                   visitCount INT,
        |                   wechatCode STRING,
        |                   weiboId STRING,
        |                   zaikanCount STRING,
        |                   zhxgChannel STRING,
        |                   mediaClassify STRING
        | ) PARTITIONED BY (`pubTime`)
        |  with(
        |     'connector' = 'hudi',
        |     'path' = '/shengbin/company',
        |     'write.tasks' = '6',
        |     'write.bucket_assign.tasks' = '6',
        |     'compaction.tasks' = '4',
        |     'table.type' = 'COPY_ON_WRITE', -- COPY_ON_WRITE (or) MERGE_ON_READ
        |     'write.bulk_insert.sort_by_partition' = 'true',  -- Whether to sort the inputs by partition path for bulk insert tasks, default true
        |     'compaction.trigger.strategy' = 'num_commits', -- 触发压缩的策略
        |     'compaction.max_memory' = '100',
        |     'compaction.async.enabled' = 'true',
        |     'write.bulk_insert.shuffle_by_partition' = 'true', -- Whether to shuffle the inputs by partition path for bulk insert tasks, default true
        |     'write.parquet.block.size' = '1024',
        |     'compaction.timeout.seconds' = '1200', -- Max timeout time in seconds for online compaction to rollback, default 20 minutes
        |     'write.sort.memory' = '10240',
        |     'write.retry.times' = '3', -- Flag to indicate how many times streaming job should retry for a failed checkpoint batch. By default 3
        |     'read.tasks' = '4', -- Parallelism of tasks that do actual read, default is 4
        |     'write.parquet.max.file.size' = '128', -- Target size for parquet files produced by Hudi write phases. For DFS, this needs to be aligned with the underlying filesystem block size for optimal performance
        |     'compaction.schedule.enabled' = 'true', -- Schedule the compaction plan, enabled by default for MOR
        |     'write.batch.size' = '256', -- Batch buffer size in MB to flush data into the underneath filesystem, default 256MB
        |     'clean.async.enabled' = 'true',
        |     'clean.retain_commits' = '10',
        |     'write.precombine' = 'true',
        |     'hoodie.datasource.write.keygenerator.class' = 'org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator',
        |     'hoodie.deltastreamer.keygen.timebased.timestamp.type' = 'EPOCHMILLISECONDS',
        |     'hoodie.deltastreamer.keygen.timebased.output.dateformat' = 'yyyyMMdd',
        |     'hoodie.embed.timeline.server' = 'false',
        |     'hive_sync.partition_extractor_class' = 'org.apache.hudi.hive.MultiPartKeysValueExtractor',
        |     'hive_sync.mode' = 'hms',
        |     'hive_sync.enable' = 'true',
        |     'hive_sync.username' = 'hdfs',
        |     'hive_sync.partition_fields' = 'pubTime',
        |     'hive_sync.table' = 'mf_company_1200_snap_test',
        |     'hive_sync.db' = 'default',
        |     'hive_sync.metastore.uris' = 'thrift://bjwfj-67p76-mediaforce-23.bfdabc.com:9083',
        |     'hive_sync.jdbc_url' = 'jdbc:hive2://bjwfj-67p77-mediaforce-24.bfdabc.com:10000',
        |     'hoodie.datasource.query.type' = 'snapshot'
        | )
        |""".stripMargin
    )
    // insert
    tableEnv.executeSql(
      s"""
         | insert into mf_company_1200_snap_hudi select * from ${table}
         |""".stripMargin
    )
  }
}
