package com.atguigu.apitest.transform.demo;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Collections;

/**
 * description:
 * Created by yqq
 * 2022-02-25
 */
public class DemoSplitStream {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /**
         * 从文件读取数据
         */
        DataStreamSource<String> inputStream = env.readTextFile("E:\\github\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        // 转换成SensorReading
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        } );

        /**
         * 分流
         */
        SplitStream<SensorReading> splitStream = dataStream.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading value) {
                return value.getTemperature() > 30 ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });


        DataStream<SensorReading> highStream = splitStream.select("high");

        DataStream<SensorReading> lowStream = splitStream.select("low");

        highStream.print("highStream--->");

//        lowStream.print("lowStream---->");

        /**
         * 将高温流转换成二元组类型，与低温流连接合并之后，输出状态信息
         */
        DataStream<Tuple2<String, Double>> warningStream = highStream.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), value.getTemperature());
            }
        });

        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStream = warningStream.connect(lowStream);

        DataStream<Object> resultStream = connectedStream.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return new Tuple3<>(value.f0, value.f1, "high temp warning");
            }

            @Override
            public Object map2(SensorReading value) throws Exception {
                return new org.apache.flink.api.java.tuple.Tuple2<>(value.getId(), "normal");
            }
        });

        resultStream.print("connect--->");


        env.execute();

    }


}
