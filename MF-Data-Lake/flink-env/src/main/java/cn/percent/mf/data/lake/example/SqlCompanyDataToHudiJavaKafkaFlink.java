package cn.percent.mf.data.lake.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author: wangshengbin
 * @date: 2022/5/11 10:39 PM
 */
public class SqlCompanyDataToHudiJavaKafkaFlink {
    public static void main(String[] args) {
        // create env
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // create table env
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, settings);

        tableEnvironment.executeSql(kafkaCompanySourceSql());
    }

    private static String kafkaCompanySourceSql() {
        return " create table k1 (\n" +
                "   docId STRING,\n" +
                "   age INT,\n" +
                "   barrageCount INT,\n" +
                "   area STRING,\n" +
                "   screen STRING,\n" +
                "   attitudesCount INT,\n" +
                "   author STRING,\n" +
                "   avatar STRING,\n" +
                "   channel STRING,\n" +
                "   city STRING,\n" +
                "   coinsCount INT,\n" +
                "   reachedRate INT,\n" +
                "   collectionCount INT,\n" +
                "   content STRING,\n" +
                "   contentHtml STRING,\n" +
                "   contentSimHash STRING,\n" +
                "   country STRING,\n" +
                "   crawlTime BIGINT,\n" +
                "   crawlTimeStr STRING,\n" +
                "   createDate STRING,\n" +
                "   createDay BIGINT,\n" +
                "   createTime BIGINT,\n" +
                "   createTimeStr STRING,\n" +
                "   dataSourceType STRING,\n" +
                "   docType STRING,\n" +
                "   fansCount INT,\n" +
                "   forwardAuthor STRING,\n" +
                "   forwardContent STRING,\n" +
                "   forwardFlag INT,\n" +
                "   forwardReleaseDate STRING,\n" +
                "   forwardUrl STRING,\n" +
                "   friendsCount INT,\n" +
                "   keyword STRING,\n" +
                "   promulgator STRING,\n" +
                "   likeCount INT,\n" +
                "   location STRING,\n" +
                "   ocr ARRAY<ROW<mfUrl STRING,content STRING,url STRING>>,\n" +
                "   subjects ARRAY<ROW<ARRAY<keywords ROW<count INT,keyword STRING>>,id BIGINT,relativity STRING,emotion DOUBLE,label2 STRING,label1 STRING,forwardContentDigest STRING,titleDigest STRING,contentDigest STRING,matchContent INT,matchOcr INT,matchTitle INT,version INT>>,\n" +
                "   originalSource STRING,\n" +
                "   originalUrl STRING,\n" +
                "   picUrls STRING,\n" +
                "   province STRING,\n" +
                "   pubDate STRING,\n" +
                "   pubDay BIGINT,\n" +
                "   pubTime BIGINT,\n" +
                "   pubTimeStr STRING,\n" +
                "   quoteCount INT,\n" +
                "   relativity STRING,\n" +
                "   repeatCount INT,\n" +
                "   commentsCount INT,\n" +
                "   replyCount INT,\n" +
                "   repostCount INT,\n" +
                "   selfMedia STRING,\n" +
                "   sex STRING,\n" +
                "   shareCount INT,\n" +
                "   interactionCount INT,\n" +
                "   coinCount INT,\n" +
                "   watchingCount INT,\n" +
                "   collectCount INT,\n" +
                "   siteDomain STRING,\n" +
                "   siteName STRING,\n" +
                "   source STRING,\n" +
                "   sysAbstract STRING,\n" +
                "   sysSentiment DOUBLE,\n" +
                "   importance DOUBLE,\n" +
                "   score DOUBLE,\n" +
                "   propagationRate DOUBLE,\n" +
                "   textArea STRING,\n" +
                "   textProvince STRING,\n" +
                "   title STRING,\n" +
                "   titleSimHash STRING,\n" +
                "   url STRING,\n" +
                "   userId STRING,\n" +
                "   userUrl STRING,\n" +
                "   verified STRING,\n" +
                "   videoImgUrl STRING,\n" +
                "   videoPicList STRING,\n" +
                "   videoUrlList STRING,\n" +
                "   videoUrls ARRAY<STRING>,\n" +
                "   wordCloud ARRAY<STRING>,\n" +
                "   visitCount INT,\n" +
                "   wechatCode STRING,\n" +
                "   weiboId STRING,\n" +
                "   zaikanCount STRING,\n" +
                "   zhxgChannel STRING,\n" +
                "   mediaClassify STRING\n" +
                " ) with(\n" +
                "     'connector' = 'kafka',\n" +
                "     'topic' = 'hudi-c-test',\n" +
                "     'properties.bootstrap.servers' = '192.168.67.124:9092,192.168.67.125:9092,192.168.67.137:9092',\n" +
                "     'properties.group.id' = 'test-sink-hudi',\n" +
                "     'scan.startup.mode' = 'earliest-offset',\n" +
                "     'format' = 'json',\n" +
                "     'json.fail-on-missing-field' = 'false',\n" +
                "     'json.ignore-parse-errors' = 'true'\n" +
                " )";
    }
}
