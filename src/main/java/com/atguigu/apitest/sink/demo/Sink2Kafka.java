package com.atguigu.apitest.sink.demo;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

/**
 * description: sink to kafka
 * Created by yqq
 * 2022-02-10
 */
public class Sink2Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String servers = "ip247:9094";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", servers);
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        /**
         * 从kafka读取数据
         */
        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer011<String>("test", new SimpleStringSchema(), properties));

        DataStream<String> dataStream = inputStream.map(line -> {
            String[] split = line.split(" ");
            return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2])).toString();
        });

        dataStream.addSink(new FlinkKafkaProducer011<String>(servers, "sinktest", new SimpleStringSchema()));
        env.execute();
    }
}
