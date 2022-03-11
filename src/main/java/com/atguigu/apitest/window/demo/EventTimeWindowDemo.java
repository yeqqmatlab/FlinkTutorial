package com.atguigu.apitest.window.demo;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * description: 事件窗口函数
 * Created by yqq
 * 2022-03-03
 */
public class EventTimeWindowDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        /**
         * 设置时间语义 : 事件时间
         *
         */
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /**
         * 设置水位线自动生成周期
         */
        env.getConfig().setAutoWatermarkInterval(100);

        // socket文本流
        DataStream<String> inputStream = env.socketTextStream("192.168.1.248", 7777);

        // 转换成SensorReading类型，分配时间戳和watermark
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        })
        //乱序数据设置时间戳和watermark
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(SensorReading element) {

                return element.getTimestamp()*1000L;
            }
        });

        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late"){};

        /**
         * 基于事件时间的开窗聚合, 统计15秒内温度的最小值
         */
        SingleOutputStreamOperator<SensorReading> minTempStream = dataStream
                .keyBy("id")
                .timeWindow(Time.seconds(15))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .minBy("temperature");

        minTempStream.print("min-->");

        minTempStream.getSideOutput(outputTag).print("late");

        env.execute();
    }
}
