package cn.percent.mf.data.lake.example;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: wangshengbin
 * @date: 2022/10/12 3:16 PM
 */
public class FlinkApiHudi {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String targetTable = "t1";
        String basePath = "file:///tmp/t1";

        Map<String, String> options = new HashMap<>();
        options.put(FlinkOptions.PATH.key(), basePath);
        options.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
        options.put(FlinkOptions.PRECOMBINE_FIELD.key(), "ts");

        DataStream<RowData> dataStream = null;
//        HoodiePipeline.Builder builder = HoodiePipeline.builder(targetTable)
//                .column("uuid VARCHAR(20)")
//                .column("name VARCHAR(10)")
//                .column("age INT")
//                .column("ts TIMESTAMP(3)")
//                .column("`partition` VARCHAR(20)")
//                .pk("uuid")
//                .partition("partition")
//                .options(options);
//
//        // The second parameter indicating whether the input data stream is bounded
//        builder.sink(dataStream, false);
        env.execute("Api_Sink");
    }
}
