package com.atguigu.apitest.sink.demo;

import com.atguigu.apitest.beans.SensorReading;
import com.atguigu.apitest.config.ConfigurationManager;
import com.atguigu.apitest.constants.Constants;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

/**
 * description: kafka sink to mysql
 * Created by yqq
 * 2022-02-10
 */
public class Sink2Mysql {
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
         * sink mysql
         */
        dataStream.addSink(new MyJBDCSink());

        env.execute();

    }

    /**
     * 自定义 SinkFunction
     */
    public static class MyJBDCSink extends RichSinkFunction<SensorReading>{

        /**
         * 声明连接和预编译sql
         */
        Connection connection = null;
        PreparedStatement insertStmt = null;
        PreparedStatement updateStmt = null;
        String url = ConfigurationManager.getProperty(Constants.JDBC_URL);
        String username = ConfigurationManager.getProperty(Constants.USERNAME);
        String password = ConfigurationManager.getProperty(Constants.PASSWORD);

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection(url, username, password);
            insertStmt = connection.prepareStatement("insert into sensor_temp (id, temp) values (?, ?)");
            updateStmt = connection.prepareStatement("update sensor_temp set temp = ? where id = ?");
        }

        /**
         *  每来一条数据，调用连接，执行sql
         * @param value
         * @param context
         * @throws Exception
         */
        @Override
        public void invoke(SensorReading value, Context context) throws Exception {

            /**
             * 直接执行更新语句，如果没有更新那么就插入
             */
            updateStmt.setDouble(1,value.getTemperature());
            updateStmt.setString(2, value.getId());
            updateStmt.execute();
            if (updateStmt.getUpdateCount() == 0) {
                insertStmt.setString(1, value.getId());
                insertStmt.setDouble(2, value.getTemperature());
                insertStmt.execute();
            }
        }

        @Override
        public void close() throws Exception {
            insertStmt.close();
            updateStmt.close();
            connection.close();
        }
    }

}
