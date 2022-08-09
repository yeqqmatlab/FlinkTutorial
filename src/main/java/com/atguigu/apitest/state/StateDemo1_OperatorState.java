package com.atguigu.apitest.state;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.Collections;
import java.util.List;

/**
 * description:
 * Created by yqq
 * 2022-08-09
 */
public class StateDemo1_OperatorState {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //socket文本流
        DataStreamSource<String> inputStream = env.socketTextStream("ip248", 7777);

        //转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
        });

        //定义一个有状态的map操作，统计当前分区个数
        DataStream<Integer> resStream = dataStream.map(new MyMapCounter());

        resStream.print();

        env.execute();
    }

    // 自定义MapFunction
    public static class MyMapCounter implements MapFunction<SensorReading, Integer>, ListCheckpointed<Integer>{

        //定义一个本地变量，作为算子状态
        private Integer count = 0;

        @Override
        public Integer map(SensorReading value) throws Exception {
            count++;
            return count;
        }

        //快照状态函数
        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(count);
        }

        //恢复状态函数
        @Override
        public void restoreState(List<Integer> state) throws Exception {
            for (Integer num : state) {
                count += num;
            }
        }
    }
}
