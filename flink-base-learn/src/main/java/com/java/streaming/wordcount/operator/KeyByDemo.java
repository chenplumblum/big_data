package com.java.streaming.wordcount.operator;

/**
 * @author chenpeibin
 * @version 1.0
 * @date 2020/7/9 10:39
 */

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

//这个例子是每行输入一个单词，以单词为key进行计数
//每10秒统计一次每个单词的个数
public class KeyByDemo {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境配置信息
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.定义加载或创建数据源（source）,监听9000端口的socket消息
        DataStream<String> textStream = env.socketTextStream("localhost", 9000, "\n");
        //3.
        DataStream<Tuple3<String, Integer, Integer>> result = textStream
                //map是将每一行单词变为一个tuple2
                .flatMap((String s, Collector<Tuple3<String, Integer, Integer>> list) -> {
                    for (String str : s.split(" ")) {
                        list.collect(new Tuple3<>(str, 1,2));
                    }
                })
                //如果要用Lambda表示是，Tuple2是泛型，那就得用returns指定类型。
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.INT))
                //keyBy进行分区，按照第一列，也就是按照单词进行分区
                .keyBy(0)
                //计算个数，计算第1列
                .sum(2);
        //4.打印输出sink
        result.print();
        //5.开始执行
        env.execute();
    }
}
