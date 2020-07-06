package com.java;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author chenpeibin
 * @version 1.0
 * @date 2020/5/28 17:01
 */
public class Hive {
    public static void main(String[] args) {
        Hive wordCount2 = new Hive();
        SparkSession sparkSession = wordCount2.init();
        String querySql = "select * from big_date.onepiece_seller_live  limit 10;";
        Dataset<Row> sql = sparkSession.sql(querySql);
        sql.printSchema();

        //关闭context
    }

    public SparkSession init(){
        String hdfsHost = "localhost";
        String hdfsPort = "8000";
        SparkConf conf = new SparkConf().setAppName("Spark WordCount Application (java)").setMaster("local[*]");
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example hive")
                .config(conf)
                .master("spark://localhost:7077")
                .enableHiveSupport()  //支持hive
                .getOrCreate();
        return spark;
    }






}
