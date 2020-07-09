package com.java.streaming.wordcount.operator;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author chenpeibin
 * @version 1.0
 * @date 2020/7/8 19:43
 */
public class MapDemo {

    private static int index = 1;
    public static void main(String[] args) throws Exception {
        //1.获取执行环境配置信息
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.定义加载或创建数据源（source）,监听9000端口的socket消息
        DataStream<String> textStream = env.socketTextStream("localhost", 9000, "\n");
        //3.map操作。
        DataStream<String> result = textStream.map(s -> (index++) + ".您输入的是：" + s);
        //4.打印输出sink
        result.print();
        //5.开始执行
        env.execute();
    }
}
