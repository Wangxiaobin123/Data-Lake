package cn.percent.mf.data.lake.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author: wangshengbin
 * @date: 2022/7/12 6:18 PM
 */
public class KafkaConsumerUtil {

    private static Properties getProperties(String kafkaBrokerList, String groupId,String offsetType) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaBrokerList);
        properties.setProperty("group.id", groupId);
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.commit.interval.ms", "5000");
        // earliest latest
        properties.put("auto.offset.reset", offsetType);
        return properties;
    }

    public static FlinkKafkaConsumer<String> getFlinkKafkaConsumer(String topic, String kafkaBrokerList, String groupId,String offsetType) {
        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>(
                topic,
                new SimpleStringSchema(),
                getProperties(kafkaBrokerList, groupId,offsetType)
        );
        flinkKafkaConsumer.setStartFromGroupOffsets();
        return flinkKafkaConsumer;
    }
}
