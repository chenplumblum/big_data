package com.java.streaming.window.window;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author chenpeibin
 * @version 1.0
 * @date 2020/7/9 15:06
 */
public class TumblingWindowsTest {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final boolean fileOutput = params.has("output");


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        final List<Tuple3<String, Long, Integer>> input = new ArrayList<>();

        input.add(new Tuple3<>("a", 10L, 1));
        input.add(new Tuple3<>("b", 10L, 1));
        input.add(new Tuple3<>("a", 19L, 1));
        input.add(new Tuple3<>("a", 20L, 1));

        input.add(new Tuple3<>("b", 30L, 1));
        input.add(new Tuple3<>("b", 50L, 1));
        input.add(new Tuple3<>("c", 60L, 1));
        // We expect to detect the session "a" earlier than this point (the old
        // functionality can only detect here when the next starts)
        input.add(new Tuple3<>("a", 100L, 1));
        // We expect to detect session "b" and "c" at this point as well
        input.add(new Tuple3<>("c", 100L, 1));

        input.add(new Tuple3<>("c", 110L, 1));
        input.add(new Tuple3<>("c", 111L, 2));
        input.add(new Tuple3<>("c", 120L, 5));

        //给源配置事件时间
        DataStream<Tuple3<String, Long, Integer>> source = env.addSource(new SourceFunction<Tuple3<String, Long, Integer>>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void run(SourceContext<Tuple3<String, Long, Integer>> ctx) throws Exception {
                for (Tuple3<String, Long, Integer> value : input) {
                    //给源设置一个时间戳
                    ctx.collectWithTimestamp(value, value.f1);
                    //给源配置水位
                    // ctx.emitWatermark(new Watermark(value.f1 - 1));
                }
                ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
            }

            @Override
            public void cancel() {
            }
        });
        // We create sessions for each id with max timeout of 3 time units
        DataStream<Tuple3<String, Long, Integer>> aggregated = source
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(20L)))
                .sum(2);

        if (fileOutput) {
            aggregated.writeAsText(params.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            aggregated.print();
        }

        env.execute();
    }
}
