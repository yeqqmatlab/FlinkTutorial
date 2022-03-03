package com.atguigu.apitest.window.demo;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * description:
 * Created by yqq
 * 2022-03-03
 */
public class CountWindowDemo {
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
         * 计数开窗函数
         */
        DataStream<Double> avgDataStream = dataStream.keyBy("id")
                .countWindow(4, 1)
                .aggregate(new AggregateFunction<SensorReading, Tuple2<Double, Integer>, Double>() {

                    /**
                     *  累加器初始化
                     * @return
                     */
                    @Override
                    public Tuple2<Double, Integer> createAccumulator() {
                        return new Tuple2<>(0.0, 0);
                    }

                    @Override
                    public Tuple2<Double, Integer> add(SensorReading value, Tuple2<Double, Integer> accumulator) {
                        // 窗口内 温度累加 计数累加
                        return new Tuple2<>(accumulator.f0 + value.getTemperature(), accumulator.f1 + 1);
                    }

                    /**
                     * 计算窗口的平均值
                     * @param accumulator
                     * @return
                     */
                    @Override
                    public Double getResult(Tuple2<Double, Integer> accumulator) {
                        return Double.valueOf(String.format("%.2f", accumulator.f0 / accumulator.f1));
                    }

                    /**
                     * 累加
                     * @param acc0
                     * @param acc1
                     * @return
                     */
                    @Override
                    public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> acc0, Tuple2<Double, Integer> acc1) {
                        return new Tuple2<>(acc0.f0 + acc1.f0, acc0.f1 + acc1.f1);
                    }
                });

        avgDataStream.print();

        env.execute();
    }

    /**
     * 自定义处理: 求相邻的四个值的平均值
     */
}
