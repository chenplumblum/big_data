package com.scale

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Wordcount {
  def main(args: Array[String]): Unit = {
    val wordcount = new SparkConf().setMaster("local[*]").setAppName("wordcount");

    val sc = new SparkContext(wordcount);

    val lines: RDD[String] = sc.textFile("file");

    val words: RDD[String] = lines.flatMap(_.split(" "));

    val wordToOne: RDD[(String, Int)] = words.map((_, 1));

    val wordToSum:RDD[(String, Int)] = wordToOne.reduceByKey(_ + _);

    val result:Array[(String, Int)] = wordToSum.collect();

    result.foreach(println(_));
  }
}
