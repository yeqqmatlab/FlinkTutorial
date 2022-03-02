package com.atguigu.apitest.transform.demo;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * description:
 * Created by yqq
 * 2022-02-25
 */
public class DemoBase {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /**
         * 从文件读取数据
         */
        DataStreamSource<String> inputStream = env.readTextFile("E:\\github\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        /**
         * 把String转换成长度输出
         */
        DataStream<Integer> mapStream = inputStream.map((MapFunction<String, Integer>) s -> s.length());

        //mapStream.print("length");

        /**
         *  flatmap 按逗号分割
         *  类似于物理量纲 单位
         *  FlatMapFunction<String, String>
         *  flink 慎用lambda表达式
         */
        FlatMapFunction<String, String> flatMapFunction = new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] fields = value.split(",");
                for (String field : fields)
                    out.collect(field);
            }
        };
        DataStream<String> flatMapStream = inputStream.flatMap(flatMapFunction);

        flatMapStream.print("flatMapStream");

        DataStream<String> filterStream = inputStream.filter((FilterFunction<String>) s -> s.startsWith("sensor_1"));
        filterStream.print("filterStream");

        env.execute();

    }


}
