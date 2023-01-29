package cn.percent.mf.data.lake.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * MySQL to Hudi
 *
 * @author: wangshengbin
 * @date: 2022/4/28 6:31 PM
 */
public class SqlToHudiExample {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        // 创建流运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        env.enableCheckpointing(5 * 1000);
        // settings
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSettings);
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        // 数据源表
        String sourceDdl = sourceMySqlSqlConnectorTable();
        // 输出表
        String sinkDdl = sinkHudiTable();

        tableEnv.executeSql(sinkDdl);
        Table t1 = tableEnv.from("t1");


        tableEnv.executeSql(sourceDdl);
        // 写入
        String transSql = "insert into t2 select * from " + t1;
        tableEnv.executeSql(transSql);
        env.execute("write");
    }

    private static String sinkHudiTable() {
        return "create table t1 (" +
                "  id int PRIMARY KEY  NOT ENFORCED,\n" +
                "  name string" +
                ") WITH(\n" +
                "'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:mysql://172.18.9.71:3306/test?autoReconnect=true',\n" +
                "  'username' = 'bfd_ibg_mf',\n" +
                "  'password' = 'bfd_ibg_mf@168',\n" +
                "  'table-name' = 't1'\n" +
                ")";
    }

    private static String sourceMySqlSqlConnectorTable() {
        return "create table t2 (\n" +
                "  id int PRIMARY KEY  NOT ENFORCED,\n" +
                "  name string" +
                ")PARTITIONED BY (`id`)\n" +
                "WITH (\n" +
                "  'connector' = 'hudi',\n" +
                "  'path'='/hudi_flink',\n" +
                "  'table.type' = 'MERGE_ON_READ'\n" +
                ")\n";
    }
}
