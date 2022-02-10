package com.atguigu.apitest.sink.demo;

import com.atguigu.apitest.beans.SensorReading;
import com.atguigu.apitest.config.ConfigurationManager;
import com.atguigu.apitest.constants.Constants;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import java.util.Properties;

/**
 * description: sink to redis
 * Created by yqq
 * 2022-02-10
 */
public class Sink2Redis {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String servers = ConfigurationManager.getProperty(Constants.BOOTSTRAP_SERVERS);
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

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] split = line.split(" ");
            return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
        });

        /**
         * sink redis
         */
        String redis_host = ConfigurationManager.getProperty(Constants.REDIS_HOST);

        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost(redis_host)
                .setPort(6379)
                .setDatabase(12)
                .build();

        dataStream.addSink(new RedisSink<SensorReading>(config, new HashRedisMapper()));

        env.execute();
    }

    /**
     * 自定义redisMapper
     * hset sensor_temp id temperature
     */
    public static class HashRedisMapper implements RedisMapper<SensorReading>{

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"sensor_temp");
        }

        @Override
        public String getKeyFromData(SensorReading sensorReading) {
            return sensorReading.getId();
        }

        @Override
        public String getValueFromData(SensorReading sensorReading) {
            return sensorReading.getTemperature().toString();
        }
    }


}
