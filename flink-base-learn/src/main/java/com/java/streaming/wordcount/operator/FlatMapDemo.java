package com.java.streaming.wordcount.operator;
import java.util.List;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author chenpeibin
 * @version 1.0
 * @date 2020/7/8 19:44
 */
public class FlatMapDemo {
    private static int index1 = 1;
    private static int index2 = 1;

    public static void main(String[] args) throws Exception {
        //1.获取执行环境配置信息
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.定义加载或创建数据源（source）,监听9000端口的socket消息
        DataStream<String> textStream = env.socketTextStream("localhost", 9000, "\n");
        //3.flatMap操作，对每一行字符串进行分割
        DataStream<String> result = textStream.flatMap((String s, Collector<String> collector) -> {
            for (String str : s.split(" ")) {
                collector.collect(str);
            }
        })
        //这个地方要注意，在flatMap这种参数里有泛型算子中。
        //如果用lambda表达式，必须将参数的类型显式地定义出来。
        //并且要有returns，指定返回的类型
        //详情可以参考Flink官方文档：https://ci.apache.org/projects/flink/flink-docs-release-1.6/dev/java_lambdas.html
        .returns(Types.STRING);

        // DataStream<Tuple2<String,Integer>> result = textStream.flatMap((String s, Collector<Tuple2<String,Integer>> list) -> {
        //     for (String str : s.split(" ")) {
        //         list.collect(new Tuple2<>(str,1));
        //     }
        // }).returns(Types.TUPLE(Types.STRING, Types.INT));


        //4.打印输出sink
        result.print();
        //5.开始执行
        env.execute();
    }
}
