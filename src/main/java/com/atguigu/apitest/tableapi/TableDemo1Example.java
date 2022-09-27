package com.atguigu.apitest.tableapi;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


/**
 * description:
 * Created by yqq
 * 2022-09-27
 */
public class TableDemo1Example {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile("E:\\github\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], new Long(split[1]), new Double(split[2]));
        });

        // 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 基于流创建一张表
        Table table = tableEnv.fromDataStream(dataStream);

//        Table resultTable = table.select("id, temperature");

        tableEnv.createTemporaryView("sensor", table);
        String sql = "select id, temperature from sensor where id = 'sensor_1' ";
        Table sqlQueryTable = tableEnv.sqlQuery(sql);

        tableEnv.toAppendStream(sqlQueryTable, Row.class).print("sql");

        env.execute();
    }
}
