package com.atguigu.apitest.window.demo;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * description:
 * Created by yqq
 * 2022-03-03
 */
public class TimeWindowDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // socket文本流
        DataStream<String> inputStream = env.socketTextStream("192.168.1.248", 7777);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(" ");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        /**
         * 时间窗口 滚动时间窗口
         */
/*        DataStream<SensorReading> time10MaxDataStream = dataStream.keyBy("id").timeWindow(Time.seconds(10)).reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading t0, SensorReading t1) throws Exception {
                return t1.getTemperature() > t0.getTemperature() ? t1 : t0;
            }
        });
        time10MaxDataStream.print("10s_max---->");*/

        DataStream<Integer> accStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(5))
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(SensorReading sensorReading, Integer accumulator) {

                        return sensorReading.getTemperature().intValue() + accumulator;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {

                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer acc1, Integer acc2) {

                        return acc1 + acc2;
                    }
                });

        accStream.print("acc--->");

        env.execute();
    }
}
