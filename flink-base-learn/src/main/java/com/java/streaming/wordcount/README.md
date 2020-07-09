# 算子
map：可以理解为映射，对每个元素进行一定的变换后，映射为另一个元素：元素个数不变
可以理解为元素的变换
flatmap:可以理解为将元素摊平，每个元素可以变为0个、1个、或者多个元素。
可以理解为元素的分割
filter:是进行筛选。
keyBy:逻辑上将Stream根据指定的Key进行分区，是根据key的散列值进行分区的。
reduce:reduce是归并操作，它可以将KeyedStream 转变为 DataStream。
fold:给定一个初始值，将各个元素逐个归并计算。它将KeyedStream转变为DataStream。