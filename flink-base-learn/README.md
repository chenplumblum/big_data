# 重点注意
1. pom文件不要使用 <scope>provided</scope>
2. print(),实际上是调用toString();方法进行打印，打印的字段顺序需要和flink的实体结构的顺序相同，不然会报错。

3. 如果用lambda表达式，必须将参数的类型显式地定义出来
```java
DataStream<Tuple2<String,Integer>> result = textStream.flatMap((String s, Collector<Tuple2<String,Integer>> list) -> {
for (String str : s.split(" ")) {
    list.collect(new Tuple2<>(str,1));
}
}).returns(Types.TUPLE(Types.STRING, Types.INT));
```