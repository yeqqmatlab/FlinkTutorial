package com.atguigu.apitest.source;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * description:
 * Created by yqq
 * 2022-02-09
 */
public class FlinkSourceDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /**
         * 数据流
         */
        DataStreamSource<Integer> intDataStream = env.fromElements(2, 4, 6, 8, 10);
        intDataStream.print("int-->");

        /**
         * 对象流
         */
        DataStreamSource<SensorReading> collection = env.fromCollection(Arrays.asList(
                new SensorReading("11", 111L, 0.9d),
                new SensorReading("12", 112L, 10d),
                new SensorReading("13", 113L, 11d)
        ));

        collection.print("data");

        
        /**
         * 从文件中读取
         */
        DataStreamSource<String> dataStreamSource = env.readTextFile("E:\\github\\FlinkTutorial\\src\\main\\resources\\sensor.txt");
        dataStreamSource.print("file--->");

        env.execute();
    }
}
