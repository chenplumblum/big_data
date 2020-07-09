# 算子
map：可以理解为映射，对每个元素进行一定的变换后，映射为另一个元素：元素个数不变
可以理解为元素的变换


flatmap:可以理解为将元素摊平，每个元素可以变为0个、1个、或者多个元素。
可以理解为元素的分割


filter:是进行筛选。


keyBy:逻辑上将Stream根据指定的Key进行分区，是根据key的散列值进行分区的。


reduce:reduce是归并操作，它可以将KeyedStream 转变为 DataStream。
需要两个参数，和返回值：.reduce((Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) -> new Tuple2(t1.f0, t1.f1 + t2.f1))

union: union可以将多个流合并到一个流中，以便对合并的流进行统一处理。是对多个流的水平拼接。参与合并的流必须是同一种类型。
测试的时候没有发现流整合


join：根据指定的Key将两个流进行关联。
