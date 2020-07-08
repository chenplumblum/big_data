# 重点注意
pom文件不要使用 <scope>provided</scope>
print(),实际上是调用toString();方法进行打印，打印的字段顺序需要和flink的实体结构的顺序相同，不然会报错。