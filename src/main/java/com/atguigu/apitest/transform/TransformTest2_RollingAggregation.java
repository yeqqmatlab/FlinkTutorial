package com.atguigu.apitest.transform;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTutorial
 * Package: com.atguigu.apitest.transform
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/11/7 15:09
 */

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import sun.awt.SunHints;

import javax.xml.crypto.Data;

/**
 * @ClassName: TransformTest2_RollingAggregation
 * @Description:
 * @Author: wushengran on 2020/11/7 15:09
 * @Version: 1.0
 */
public class TransformTest2_RollingAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("E:\\github\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        // 转换成SensorReading类型
//        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
//            @Override
//            public SensorReading map(String value) throws Exception {
//                String[] fields = value.split(",");
//                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
//            }
//        });

        DataStream<SensorReading> dataStream = inputStream.map( line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        } );

//        dataStream.print("--->");

        // 分组
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");
        KeyedStream<SensorReading, String> keyedStream1 = dataStream.keyBy(data -> data.getId());


        DataStream<Long> dataStream1 = env.fromElements(1L, 34L, 4L, 657L, 23L);
        KeyedStream<Long, Integer> keyedStream2 = dataStream1.keyBy(new KeySelector<Long, Integer>() {
            @Override
            public Integer getKey(Long value) throws Exception {
                return value.intValue() % 2;
            }
        });

        keyedStream2.print("---2--");

        //        KeyedStream<SensorReading, String> keyedStream1 = dataStream.keyBy(SensorReading::getId);

        // 滚动聚合，取当前key最大的温度值
        DataStream<SensorReading> resultStream = keyedStream1.maxBy("temperature");

        resultStream.print("result");

//
//        keyedStream1.print("key1");
//        keyedStream2.sum(0).print("key2");
        env.execute();
    }
}
