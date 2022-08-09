package com.atguigu.apitest.sink.demo;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * description:
 * Created by yqq
 * 2022-03-02
 */
public class Sink2ES {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("E:\\github\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //定义es的连接配置
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("localhost", 9200));

        dataStream.addSink(new ElasticsearchSink.Builder<SensorReading>(httpHosts, new MyEsSinkFunction()).build());

        env.execute();

    }

    /**
     * 实现自定义的es写入操作
     */
    public static class MyEsSinkFunction implements ElasticsearchSinkFunction<SensorReading>{
        @Override
        public void process(SensorReading ele, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
            // 定义写入的数据source
            Map<String, String> dataSource = new HashMap<>();
            dataSource.put("id", ele.getId());
            dataSource.put("ts", ele.getTimestamp().toString());
            dataSource.put("temp", ele.getTemperature().toString());

            IndexRequest indexRequest = Requests.indexRequest().index("sensor").type("_doc").source(dataSource);

            requestIndexer.add(indexRequest);

        }
    }

}
