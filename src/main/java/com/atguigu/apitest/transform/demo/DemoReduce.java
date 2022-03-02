package com.atguigu.apitest.transform.demo;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import javax.xml.crypto.Data;

/**
 * description:
 * Created by yqq
 * 2022-02-25
 */
public class DemoReduce {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /**
         * 从文件读取数据
         */
        DataStreamSource<String> inputStream = env.readTextFile("E:\\github\\FlinkTutorial\\src\\main\\resources\\sensor.txt");


        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
            }
        });

        dataStream.print();

        /**
         * reduce
         */

        DataStream<SensorReading> newDataStream = dataStream.keyBy(SensorReading::getId).reduce((ReduceFunction<SensorReading>) (curData, newData) -> new SensorReading(curData.getId(), newData.getTimestamp(), Math.max(curData.getTemperature(), newData.getTemperature())));

        newDataStream.print("new_data--->");


        env.execute();

    }


}
