package cn.percent.mf.data.lake.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author: wangshengbin
 * @date: 2022/5/11 10:39 PM
 */
public class SqlToHudiJavaKafkaFlink {
    public static void main(String[] args) {
        // create env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // create table env
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, settings);

        tableEnvironment.executeSql(kafkaSourceSql());
    }

    private static String kafkaSourceSql() {
        return " create table k1 (\n" +
                "   docId STRING,\n" +
                "   age INT,\n" +
                "   area STRING,\n" +
                "   attitudesCount INT,\n" +
                "   author STRING,\n" +
                "   avatar STRING,\n" +
                "   biz STRING,\n" +
                "   btTaskId STRING,\n" +
                "   bulletCount INT,\n" +
                "   channel STRING,\n" +
                "   city STRING,\n" +
                "   coinsCount INT,\n" +
                "   collectionCount INT,\n" +
                "   content STRING,\n" +
                "   contentHtml STRING,\n" +
                "   contentSimHash STRING,\n" +
                "   country STRING,\n" +
                "   crawlDate STRING,\n" +
                "   crawlDay BIGINT,\n" +
                "   crawlTime BIGINT,\n" +
                "   crawlTimeStr STRING,\n" +
                "   createDate STRING,\n" +
                "   createDay BIGINT,\n" +
                "   createTime BIGINT,\n" +
                "   createTimeStr STRING,\n" +
                "   dataSourceType STRING,\n" +
                "   docType STRING,\n" +
                "   emotionEntry ARRAY<ROW<offSet STRING,entryName STRING,emotionValue DOUBLE>>,\n" +
                "   fansCount STRING,\n" +
                "   finalPhrase STRING,\n" +
                "   forwardAuthor STRING,\n" +
                "   forwardContent STRING,\n" +
                "   forwardFlag INT,\n" +
                "   forwardPics STRING,\n" +
                "   forwardPictureList STRING,\n" +
                "   forwardReferUrl STRING,\n" +
                "   forwardReleaseDate STRING,\n" +
                "   forwardUid STRING,\n" +
                "   forwardUrl STRING,\n" +
                "   forwardUserUrl STRING,\n" +
                "   friendsCount INT,\n" +
                "   hlAbstract STRING,\n" +
                "   hlKeywords STRING,\n" +
                "   hyTaskType STRING,\n" +
                "   hylandaDocId STRING,\n" +
                "   keyword STRING,\n" +
                "   kid STRING,\n" +
                "   likeCount INT,\n" +
                "   location STRING,\n" +
                "   manualSentiment STRING,\n" +
                "   nodupUrlKey STRING,\n" +
                "   ocr ARRAY<ROW<mfUrl STRING,content STRING,url STRING,>>,\n" +
                "   originDocId STRING,\n" +
                "   originaUrl STRING,\n" +
                "   originalContentSimHash STRING,\n" +
                "   originalFrom STRING,\n" +
                "   originalPhrase STRING,\n" +
                "   originalSource STRING,\n" +
                "   originalTitleSimHash STRING,\n" +
                "   originalUrl STRING,\n" +
                "   picUrls STRING,\n" +
                "   pictureList STRING,\n" +
                "   postSource STRING,\n" +
                "   post_id STRING,\n" +
                "   postsCount STRING,\n" +
                "   province STRING,\n" +
                "   pubDate STRING,\n" +
                "   pubDay BIGINT,\n" +
                "   pubTime BIGINT,\n" +
                "   pubTimeStr STRING,\n" +
                "   quoteCount INT,\n" +
                "   relativity STRING,\n" +
                "   relavancy STRING,\n" +
                "   repeatCount INT,\n" +
                "   replyCount INT,\n" +
                "   repostCount INT,\n" +
                "   selfMedia STRING,\n" +
                "   sex STRING,\n" +
                "   shareCount INT,\n" +
                "   siteDomain STRING,\n" +
                "   siteName STRING,\n" +
                "   source STRING,\n" +
                "   sourceUrl STRING,\n" +
                "   spamTag STRING,\n" +
                "   subjectKeyName STRING,\n" +
                "   subjectRawName STRING,\n" +
                "   subjectType STRING,\n" +
                "   sysAbstract STRING,\n" +
                "   sysKeywords STRING,\n" +
                "   sysSentiment DOUBLE,\n" +
                "   sysSentimentTag STRING,\n" +
                "   textArea STRING,\n" +
                "   textProvince STRING,\n" +
                "   title STRING,\n" +
                "   titleLength STRING,\n" +
                "   titleSimHash STRING,\n" +
                "   url STRING,\n" +
                "   urlHasg STRING,\n" +
                "   userId STRING,\n" +
                "   userUrl STRING,\n" +
                "   verified STRING,\n" +
                "   videoImgUrl STRING,\n" +
                "   videoPicList STRING,\n" +
                "   videoUrlList STRING,\n" +
                "   videoUrls ARRAY<STRING>,\n" +
                "   visitCount STRING,\n" +
                "   wechatCode STRING,\n" +
                "   weiboId STRING,\n" +
                "   weiboMid STRING,\n" +
                "   weixinName STRING,\n" +
                "   zaikanCount STRING,\n" +
                "   zhxgChannel STRING,\n" +
                "   zhxgDataType STRING,\n" +
                "   zhxgKeyWords STRING,\n" +
                "   zhxgSubjectId STRING,\n" +
                "   zhxgSubjectName STRING\n" +
                " ) with(\n" +
                "     'connector' = 'kafka',\n" +
                "     'topic' = 'mf.streaming.data.realtime.v1',\n" +
                "     'properties.bootstrap.servers' = '192.168.67.124:9092,192.168.67.125:9092,192.168.67.137:9092',\n" +
                "     'properties.group.id' = 'test-flink-con',\n" +
                "     'scan.startup.mode' = 'earliest-offset',\n" +
                "     'format' = 'json',\n" +
                "     'json.fail-on-missing-field' = 'false',\n" +
                "     'json.ignore-parse-errors' = 'true'\n" +
                " )";
    }
}
