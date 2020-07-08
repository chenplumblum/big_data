# 窗口
## 滚动窗口
```java
DataStream<T> input = ...;

// 滚动窗口-事件时间
input
    .keyBy(<key selector>)
    .window(TumblingEventTimeWindows.of(Time.seconds(5)))
    .<windowed transformation>(<window function>);

// 滚动窗口-处理时间
input
    .keyBy(<key selector>)
    .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
    .<windowed transformation>(<window function>);

// 滚动窗口-事件时间并设置8小时的偏移量（用于抵消时差）
input
    .keyBy(<key selector>)
    // 偏移量15分钟：1:00:00.000 - 1:59:59.999 -> 1:15:00.000 - 2:14:59.999
    // 在中国，必须指定偏移量Time.hours(-8) （中国是UTC +8 ，flink默认使用UTC-0）
    .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
    .<windowed transformation>(<window function>);
```
## 滑动窗口
```java
DataStream<T> input = ...;

// 滑动窗口-事件时间
input
    .keyBy(<key selector>)
    .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
    .<windowed transformation>(<window function>);

// 滑动窗口-处理时间
input
    .keyBy(<key selector>)
    .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
    .<windowed transformation>(<window function>);

// 滑动窗口-事件时间并设置8小时的偏移量（用于抵消时差）
input
    .keyBy(<key selector>)
    .window(SlidingProcessingTimeWindows.of(Time.hours(12), Time.hours(1), Time.hours(-8)))
    .<windowed transformation>(<window function>);

```
## 会话窗口
```java
DataStream<T> input = ...;

// 会话窗口 - 事件时间 - 静态会话间隙
input
    .keyBy(<key selector>)
    .window(EventTimeSessionWindows.withGap(Time.minutes(10)))
    .<windowed transformation>(<window function>);

// 会话窗口 - 事件时间 - 动态会话间隙函数
input
    .keyBy(<key selector>)
    .window(EventTimeSessionWindows.withDynamicGap((element) -> {
        // determine and return session gap
    }))
    .<windowed transformation>(<window function>);

// 会话窗口 -处理时间 - 静态会话间隙
input
    .keyBy(<key selector>)
    .window(ProcessingTimeSessionWindows.withGap(Time.minutes(10)))
    .<windowed transformation>(<window function>);

// 会话窗口 - 处理时间 - 动态会话间隙函数
input
    .keyBy(<key selector>)
    .window(ProcessingTimeSessionWindows.withDynamicGap((element) -> {
        // determine and return session gap
    }))
    .<windowed transformation>(<window function>);

```

# 重点
滚动窗口：一开始就是windowSize大小，并移动windowSize
滑动窗口：一开始为最小的单位，直到涨到windowSize，并移动slideSize
    注意：如果slideSize>windowSize,相当于ABC，每次只取BC