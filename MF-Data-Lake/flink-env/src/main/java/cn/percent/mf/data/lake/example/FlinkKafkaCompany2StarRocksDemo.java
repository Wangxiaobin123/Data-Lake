package cn.percent.mf.data.lake.example;

import cn.hutool.core.date.DateUtil;
import cn.percent.mf.data.lake.util.KafkaConsumerUtil;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.time.Duration;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author: wangshengbin
 * @date: 2022/11/15 11:56 PM
 */
public class FlinkKafkaCompany2StarRocksDemo {
    private static final String ERROR_DATE = "1970-01-01";
    private static final Long DAY_50 = 1000 * 60 * 60 * 24 * 50L;
    private static final Long DAY = 1000 * 60 * 60 * 24L;

    public static void main(String[] args) {
        StreamExecutionEnvironment env = getExecutionEnvironment();
        String groupId = "mf.test.doris.g.2";
        String topic = "mf.company.test.doris";
        String brokers = "192.168.67.124:9092,192.168.67.125:9092,192.168.67.137:9092";

        FlinkKafkaConsumer<String> myConsumer = KafkaConsumerUtil
                .getFlinkKafkaConsumer(topic, brokers, groupId,"earliest");

        myConsumer.assignTimestampsAndWatermarks(
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)));

        DataStream<String> source = env
                .addSource(myConsumer)
                .uid("kafka-source-1").name("k1")
                .setParallelism(1);
        SingleOutputStreamOperator<JSONObject> streamOperator = source.map((MapFunction<String, JSONObject>) JSONObject::parseObject);
        SingleOutputStreamOperator<JSONObject> dataFilter = streamOperator.filter(new FilterFunction<JSONObject>() {
            private static final long serialVersionUID = 5866682014461234067L;

            @Override
            public boolean filter(JSONObject value) {
                long pubTime = value.getLongValue("pubTime");
                Date pubDay = new Date(pubTime);
                return pubTime > (System.currentTimeMillis() - DAY * 400) && pubTime < (System.currentTimeMillis() + DAY * 3);
            }
        });
        SingleOutputStreamOperator<String> operator = dataFilter.map(new MapFunction<JSONObject, String>() {
            private static final long serialVersionUID = 5855266996792786460L;

            @Override
            public String map(JSONObject value) {
                JSONObject result = new JSONObject(true);
                Date pubDay = new Date(value.getLongValue("pubTime"));
                String formatDate = DateUtil.formatDate(pubDay);
                result.put("pub_day", formatDate);
                for (String key : PARAM_MAP.keySet()) {
                    result.put(key, value.get(PARAM_MAP.get(key)));
                }
                return result.toJSONString();
            }
        }).uid("json-to-string-2").name("format");

        operator
                .addSink(
                        StarRocksSink.sink(
                                StarRocksSinkOptions.builder()
                                        .withProperty("jdbc-url", "jdbc:mysql://192.168.67.131:9030/example_db")
                                        .withProperty("load-url", "192.168.67.131:8030")
                                        .withProperty("username", "root")
                                        .withProperty("password", "")
                                        .withProperty("table-name", "mf_company_ods_958")
                                        .withProperty("database-name", "example_db")
                                        .withProperty("sink.properties.format", "json")
                                        .withProperty("sink.properties.strip_outer_array", "true")
                                        // in case of raw data contains common delimiter like '\n'
                                        .withProperty("sink.properties.row_delimiter", "\\x02")
                                        // in case of raw data contains common separator like '\t'
                                        .withProperty("sink.properties.column_separator", "\\x01")
                                        .withProperty("sink.buffer-flush.interval-ms", "15000")
                                        .withProperty("sink.max-retries", "3")
                                        .withProperty("sink.connect.timeout-ms", "20000")
                                        .build()
                        )
                )
                .uid("sourceSink-uid").name("sourceSink");

        try {
            env.execute("StarRocksSink_StringJava");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static StreamExecutionEnvironment getExecutionEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setMaxParallelism(3);
        env.setParallelism(3);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                // failureRate
                3,
                // failureInterval
                org.apache.flink.api.common.time.Time.of(5, TimeUnit.MINUTES),
                // delayInterval
                org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)
        ));
        // checkpoint options
        env.enableCheckpointing(1000 * 30);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(1000 * 60 * 10);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(Integer.MAX_VALUE);
        return env;
    }

    private static final Map<String, String> PARAM_MAP = Maps.newLinkedHashMap();

    static {
        PARAM_MAP.put("doc_id", "docId");
        PARAM_MAP.put("weibo_mid", "weiboMid");
        PARAM_MAP.put("barrage_count", "barrageCount");
        PARAM_MAP.put("like_count", "likeCount");
        PARAM_MAP.put("quote_count", "quoteCount");
        PARAM_MAP.put("share_count", "shareCount");
        PARAM_MAP.put("posts_count", "");
        PARAM_MAP.put("repeat_count", "repeatCount");
        PARAM_MAP.put("attitudes_count", "attitudesCount");
        PARAM_MAP.put("visit_count", "visitCount");
        PARAM_MAP.put("repost_count", "repostCount");
        PARAM_MAP.put("coin_count", "coinCount");
        PARAM_MAP.put("watching_count", "watchingCount");
        PARAM_MAP.put("friends_count", "friendsCount");
        PARAM_MAP.put("reply_count", "replyCount");
        PARAM_MAP.put("comments_count", "commentsCount");
        PARAM_MAP.put("crawl_keyword", "crawlKeyword");
        PARAM_MAP.put("crawl_url", "crawlUrl");
        PARAM_MAP.put("crawl_account", "crawlAccount");
        PARAM_MAP.put("title_simHash", "titleSimHash");
        PARAM_MAP.put("content_html", "contentHtml");
        PARAM_MAP.put("pic_urls", "picUrls");
        PARAM_MAP.put("original_source", "originalSource");
        PARAM_MAP.put("refer_url", "referUrl");
        PARAM_MAP.put("content_simHash", "contentSimHash");
        PARAM_MAP.put("publisher_name", "publisherName");
        PARAM_MAP.put("ip_region", "ipRegion");
        PARAM_MAP.put("data_source_type", "dataSourceType");
        PARAM_MAP.put("forward_content", "forwardContent");
        PARAM_MAP.put("site_name", "siteName");
        PARAM_MAP.put("forward_user_url", "forwardUserUrl");
        PARAM_MAP.put("publisher_id", "publisherId");
        PARAM_MAP.put("forward_flag", "forwardFlag");
        PARAM_MAP.put("text_province", "textProvince");
        PARAM_MAP.put("text_area", "textArea");
        PARAM_MAP.put("video_urls", "videoUrls");
        PARAM_MAP.put("forward_author", "forwardAuthor");
        PARAM_MAP.put("forward_url", "forwardUrl");
        PARAM_MAP.put("interaction_count", "interactionCount");
        PARAM_MAP.put("doc_type", "docType");
        PARAM_MAP.put("collect_count", "collectCount");
        PARAM_MAP.put("interaction_update_time", "interactionUpdateTime");
        PARAM_MAP.put("user_id", "userId");
        PARAM_MAP.put("zhxg_subject_id", "zhxgSubjectId");
        PARAM_MAP.put("hl_keywords", "hlKeywords");
        PARAM_MAP.put("create_time", "createTime");
        PARAM_MAP.put("weibo_id", "weiboId");
        PARAM_MAP.put("ocr_update_time", "ocrUpdateTime");
        PARAM_MAP.put("spam_tag", "spamTag");
        PARAM_MAP.put("publisher_platform", "publisherPlatform");
        PARAM_MAP.put("pub_time", "pubTime");
        PARAM_MAP.put("crawl_time", "crawlTime");
        PARAM_MAP.put("real_create_time", "realCreateTime");
        PARAM_MAP.put("fans_count", "fansCount");
        PARAM_MAP.put("rel_type", "relType");
        PARAM_MAP.put("subject_type", "subjectType");
        PARAM_MAP.put("source_url", "sourceUrl");
        PARAM_MAP.put("user_url", "userUrl");
        PARAM_MAP.put("picture_list", "pictureList");
        PARAM_MAP.put("video_url_list", "videoUrlList");
        PARAM_MAP.put("subject_raw_name", "subjectRawName");
        PARAM_MAP.put("sys_sentiment", "sysSentiment");
        PARAM_MAP.put("nodup_url_key", "nodupUrlKey");
        PARAM_MAP.put("site_domain", "siteDomain");
        PARAM_MAP.put("origina_url", "originaUrl");
        PARAM_MAP.put("zhxg_data_type", "zhxgDataType");
        PARAM_MAP.put("weixin_name", "weixinName");
        PARAM_MAP.put("self_media", "selfMedia");
        PARAM_MAP.put("url_hash", "urlHash");
        PARAM_MAP.put("post_source", "postSource");
        PARAM_MAP.put("subject_key_name", "subjectKeyName");
        PARAM_MAP.put("wechat_code", "wechatCode");
        PARAM_MAP.put("zhxg_subject_name", "zhxgSubjectName");
        PARAM_MAP.put("url", "url");
        PARAM_MAP.put("ocr", "ocr");
        PARAM_MAP.put("channel", "channel");
        PARAM_MAP.put("city", "city");
        PARAM_MAP.put("country", "country");
        PARAM_MAP.put("province", "province");
        PARAM_MAP.put("relavancy", "relavancy");
        PARAM_MAP.put("sex", "sex");
        PARAM_MAP.put("avatar", "avatar");
        PARAM_MAP.put("language", "language");
        PARAM_MAP.put("source", "source");
        PARAM_MAP.put("area", "area");
        PARAM_MAP.put("post_id", "post_id");
        PARAM_MAP.put("relativity", "relativity");
        PARAM_MAP.put("title", "title");
        PARAM_MAP.put("content", "content");
        PARAM_MAP.put("verified", "verified");
        PARAM_MAP.put("location", "location");
        PARAM_MAP.put("age", "age");

        PARAM_MAP.put("screen", "screen");
        PARAM_MAP.put("promulgator", "promulgator");
        PARAM_MAP.put("score", "score");
        PARAM_MAP.put("word_cloud", "wordCloud");
        PARAM_MAP.put("importance", "importance");
        PARAM_MAP.put("propagation_rate", "propagationRate");
        PARAM_MAP.put("subjects", "subjects");
        PARAM_MAP.put("media_classify", "mediaClassify");
        PARAM_MAP.put("content_length", "contentLength");
    }
}
