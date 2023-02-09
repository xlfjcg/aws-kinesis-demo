package com.starrocks.demo.flink.kinesis;

import com.starrocks.connector.flink.table.data.StarRocksRowData;
import com.starrocks.connector.flink.table.sink.StarRocksDynamicSinkFunctionV2;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.demo.flink.entity.DefaultStarRocksRowData;
import com.starrocks.demo.flink.func.DebeziumMapFunction;
import com.starrocks.demo.flink.util.SqlQueryProvider;
import com.starrocks.demo.flink.util.StarRocksUtils;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class SimpleMysqlToStarRocks {

    private static final String MYSQL_HOST = "yizhou-dms-source.xxxx.ap-southeast-1.rds.amazonaws.com";
    private static final int MYSQL_PORT = 3306;
    private static final String MYSQL_USERNAME = "admin";
    private static final String MYSQL_PASSWORD = "12345678";

    private static DataStream<String> createSource(StreamExecutionEnvironment env) {
        Properties props = new Properties();
        props.setProperty("decimal.handling.mode", "STRING");
        props.setProperty("bigint.unsigned.handling.mode", "long");
        props.setProperty("scan.startup.mode", "initial");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(MYSQL_HOST)
                .port(MYSQL_PORT)
                .databaseList("m_demo")
                .tableList("m_demo.t_source")
                .username(MYSQL_USERNAME)
                .password(MYSQL_PASSWORD)
                .startupOptions(StartupOptions.initial())
                .debeziumProperties(props)
                .deserializer(new JsonDebeziumDeserializationSchema(true))
                .serverTimeZone("UTC")
                .build();

        return env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql").setParallelism(1);
    }

    private static SinkFunction<StarRocksRowData> createSink() {
        StarRocksSinkOptions sinkOptions = StarRocksSinkOptions.builder()
                .withProperty("load-url", "https://sclmam06e.app.cloud.celerdata.xyz")
                .withProperty("jdbc-url", "jdbc:mysql://sclmam06e.app.cloud.celerdata.xyz:9030")
                .withProperty("username", "admin")
                .withProperty("password", "test")
                .withProperty("database-name", "m_demo")
                .withProperty("table-name", "t_source")
                .withProperty("sink.connect.timeout-ms", "60000")
                .withProperty("sink.buffer-flush.interval-ms", "30000")
                .withProperty("sink.properties.timeout", "25920")
                .withProperty("sink.properties.format", "json")
                .withProperty("sink.properties.strip_outer_array", "true")
                .withProperty("sink.properties.ignore_json_size", "true")
                .build();
        return new StarRocksDynamicSinkFunctionV2<>(sinkOptions);
    }

    public static void main(String[] args) throws Exception {
        SqlQueryProvider queryProvider = new SqlQueryProvider(MYSQL_HOST, MYSQL_PORT, MYSQL_USERNAME, MYSQL_PASSWORD);

        List<Map<String, String>> result = queryProvider.execute("select TABLE_SCHEMA, TABLE_NAME, group_concat(COLUMN_NAME) as COLUMN_NAMES from `information_schema`.`COLUMNS` where COLUMN_KEY = 'PRI' group by TABLE_SCHEMA, TABLE_NAME;");

        Map<String, String[]> primaryKeysMap = result.stream()
                .collect(
                        Collectors.toMap(
                                kv -> StarRocksUtils.getTableKey(kv.get("TABLE_SCHEMA"), kv.get("TABLE_NAME")),
                                kv -> kv.get("COLUMN_NAMES").split(",")
                        )
                );


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> input = createSource(env);
        input.map(new DebeziumMapFunction(primaryKeysMap))
                .keyBy(new KeySelector<StarRocksRowData, Object>() {
                    @Override
                    public Object getKey(StarRocksRowData starRocksRowData) throws Exception {
                        return ((DefaultStarRocksRowData) starRocksRowData).getKeyBy();
                    }
                })
                .addSink(createSink());

        env.execute("SimpleMysqlToStarRocks");
    }
}
