package com.starrocks.demo.flink.kinesis;

import com.starrocks.connector.flink.table.sink.StarRocksDynamicSinkFunctionV2;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import java.util.Properties;

public class KinesisToStarRocks {

    private static final String region = "ap-southeast-1";
    private static final String streamName = "ExampleStream";

    private static DataStream<String> createSourceFromStaticConfig(StreamExecutionEnvironment env) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfigConstants.AWS_REGION, region);
        properties.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, ConsumerConfigConstants.InitialPosition.TRIM_HORIZON.toString());

        return env.addSource(new FlinkKinesisConsumer<>(streamName, new SimpleStringSchema(), properties));
    }

    private static SinkFunction<String> createSink() {
        StarRocksSinkOptions sinkOptions = StarRocksSinkOptions.builder()
                .withProperty("load-url", "https://xxxx.app.cloud.celerdata.xyz")
                .withProperty("jdbc-url", "jdbc:mysql://xxxx.app.cloud.celerdata.xyz:9030")
                .withProperty("username", "admin")
                .withProperty("password", "test")
                .withProperty("database-name", "sr_demo")
                .withProperty("table-name", "lineorder_flat")
                .withProperty("sink.connect.timeout-ms", "60000")
                .withProperty("sink.buffer-flush.interval-ms", "30000")
                .withProperty("sink.properties.timeout", "25920")
                .withProperty("sink.properties.column_separator", "|")
                .build();

        return new StarRocksDynamicSinkFunctionV2<>(sinkOptions);
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> input = createSourceFromStaticConfig(env);
        input.addSink(createSink());

        env.execute("sr demo KinesisToStarRocks");
    }
}