package com.starrocks.demo.flink.kinesis;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink;
import org.apache.flink.kinesis.shaded.org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class WriteToKinesis {
    private static final String region = "ap-southeast-1";
    private static final String streamName = "ExampleStream";

    private static DataStream<String> createSourceFromResource(StreamExecutionEnvironment env) {
        try (InputStream is = WriteToKinesis.class.getClassLoader().getResourceAsStream("data.csv")) {
            assert is != null;
            String content = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            System.out.println(content);
            return env.fromElements(content.split(System.lineSeparator()));
        } catch (Exception ignored) {

        }

        return null;
    }

    private static KinesisStreamsSink<String> createSinkFromStaticConfig() {
        Properties properties = new Properties();
        properties.setProperty(AWSConfigConstants.AWS_REGION, region);

        return KinesisStreamsSink.<String>builder()
                .setKinesisClientProperties(properties)
                .setSerializationSchema(new SimpleStringSchema())
                .setStreamName(streamName)
                .setPartitionKeyGenerator(e -> String.valueOf(e.hashCode()))
                .build();
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> input = createSourceFromResource(env);
        assert input != null;
        input.sinkTo(createSinkFromStaticConfig());
        env.execute("sr demo WriteToKinesis");
    }
}
