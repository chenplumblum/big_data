package com.java;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * @author chenpeibin
 * @version 1.0
 * @date 2020/5/28 17:01
 */
public class WordCount2 {
    public static void main(String[] args) {
        WordCount2 wordCount2 = new WordCount2();
        JavaSparkContext javaSparkContext = wordCount2.init();
        String hdfsHost = "localhost";
        String hdfsPort = "8000";

        String hdfsBasePath = "hdfs://" + hdfsHost + ":" + hdfsPort;

        //输出结果文件的hdfs路径
        String outputPath = hdfsBasePath + "/output/"
                + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());

        System.out.println("output path : " + outputPath);
        wordCount2.mkdir(javaSparkContext,outputPath);
        //关闭context
        wordCount2.close(javaSparkContext);
    }

    public JavaSparkContext init(){
        String hdfsHost = "localhost";
        String hdfsPort = "8000";
        SparkConf sparkConf = new SparkConf().setAppName("Spark WordCount Application (java)").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        return javaSparkContext;
    }


    public void close(JavaSparkContext javaSparkContext){
        javaSparkContext.close();
    }
    public Boolean mkdir(JavaSparkContext javaSparkContext,String outputPath){
        List<Tuple2<Integer, String>> list = new ArrayList<>();
        Tuple2 tuple1 = new Tuple2(1,"test1");
        Tuple2 tuple2 = new Tuple2(2,"test2");
        Tuple2 tuple3 = new Tuple2(3,"test3");
        Tuple2 tuple4 = new Tuple2(4,"test4");
        Tuple2 tuple5 = new Tuple2(5,"test5");
        Tuple2 tuple6 = new Tuple2(6,"test6");
        list.add(tuple1);
        list.add(tuple2);
        list.add(tuple3);
        list.add(tuple4);
        list.add(tuple5);
        list.add(tuple6);
        //分区合并成一个，再导出为一个txt保存在hdfs
        javaSparkContext.parallelize(list).saveAsTextFile(outputPath);
        return true;

    }

    public JavaRDD<String> readdir(JavaSparkContext javaSparkContext,String inputPath){
        JavaRDD<String> textFile = javaSparkContext.textFile(inputPath);
        return textFile;
    }



}
