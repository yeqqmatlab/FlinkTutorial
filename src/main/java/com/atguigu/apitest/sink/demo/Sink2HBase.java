package com.atguigu.apitest.sink.demo;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * description:
 * Created by yqq
 * 2022-03-02
 */
public class Sink2HBase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        env.execute();
    }
}
