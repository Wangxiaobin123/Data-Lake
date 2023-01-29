package cn.percent.mf.data.lake.example;

import cn.hutool.core.util.ObjectUtil;
import cn.percent.mf.data.lake.util.KafkaConsumerUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase.FlushBackoffType.CONSTANT;

/**
 * @author: wangshengbin
 * @date: 2022/11/15 11:56 PM
 */
public class FlinkBkafkaEsDemo {
    private static final String ERROR_DATE = "1970-01-01";
    private static final Long DAY_400 = 1000 * 60 * 60 * 24 * 400L;

    public static void main(String[] args) throws UnknownHostException {
        StreamExecutionEnvironment env = getExecutionEnvironment();
        String groupId = "ods.zhxg.check.cache.test";
        String topic = "mf.streaming.data.crawl.v1.cache";
        String brokers = "192.168.67.124:9092,192.168.67.125:9092,192.168.67.137:9092";

        FlinkKafkaConsumer<String> myConsumer = KafkaConsumerUtil
                .getFlinkKafkaConsumer(topic, brokers, groupId, "latest");

        myConsumer.assignTimestampsAndWatermarks(
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)));

        DataStream<String> source = env
                .addSource(myConsumer)
                .uid("kafka-source-1").name("k1")
                .setParallelism(1);
        SingleOutputStreamOperator<JSONObject> streamOperator = source.map((MapFunction<String, JSONObject>) JSONObject::parseObject);

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("192.168.67.83", 9700, "http"));
        httpHosts.add(new HttpHost("192.168.67.85", 9700, "http"));

        int esFieldsSize = 20000;
        // use a ElasticsearchSink.Builder to create an ElasticsearchSink
        ElasticsearchSink.Builder<JSONObject> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<JSONObject>() {
                    private static final long serialVersionUID = -8300301130256922120L;

                    public IndexRequest createIndexRequest(JSONObject element) {
                        return Requests.indexRequest()
                                .index("b-data-test-1")
                                .source(element);
                    }

                    @Override
                    public void process(JSONObject element, RuntimeContext ctx, RequestIndexer indexer) {
                        if (ObjectUtil.isNotNull(element) && element.size() <= esFieldsSize) {
                            JSONObject userLogic = element.getJSONObject("user_logic");
                            JSONArray result = userLogic.getJSONArray("result");
                            result.getJSONObject(0).remove("word_pos");
                            indexer.add(createIndexRequest(element));
                        }
                    }
                }
        );

        // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
        esSinkBuilder.setBulkFlushMaxActions(500);

        // provide a RestClientFactory for custom configuration on the internally created REST client
        esSinkBuilder.setRestClientFactory(
                restClientBuilder -> {
                    Header[] headers = new BasicHeader[]{new BasicHeader("Content-Type", "application/json")};
                    restClientBuilder.setDefaultHeaders(headers);
                }
        );
        // 刷新前缓冲的最大动作量
        esSinkBuilder.setBulkFlushMaxActions(3);
        // 刷新前缓冲区的最大数据大小（以MB为单位）
        esSinkBuilder.setBulkFlushMaxSizeMb(5);
        // 论缓冲操作的数量或大小如何都要刷新的时间间隔
        esSinkBuilder.setBulkFlushInterval(5000);
        esSinkBuilder.setBulkFlushBackoffRetries(3);
        esSinkBuilder.setBulkFlushBackoffType(CONSTANT);
        esSinkBuilder.setFailureHandler(new ActionRequestFailureHandler() {
            private static final long serialVersionUID = -2280734939132936550L;

            @Override
            public void onFailure(ActionRequest actionRequest, Throwable throwable, int i, RequestIndexer requestIndexer) {
                System.out.println(throwable.getMessage() + "----" + actionRequest.toString());
            }
        });
        // finally, build and add the sink to the job's pipeline
        streamOperator.addSink(esSinkBuilder.build());
        try {
            env.execute("B-Data-test");
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
}
