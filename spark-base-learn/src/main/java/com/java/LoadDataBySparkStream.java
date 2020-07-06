// package com.java;
//
// import java.util.ArrayList;
// import java.util.Date;
// import java.util.HashMap;
// import java.util.HashSet;
// import java.util.List;
// import java.util.Map;
// import java.util.Set;
//
// import kafka.serializer.StringDecoder;
// import org.apache.hadoop.fs.LocalFileSystem;
// import org.apache.hadoop.hdfs.DistributedFileSystem;
// import org.apache.spark.SparkConf;
// import org.apache.spark.api.java.JavaPairRDD;
// import org.apache.spark.api.java.JavaSparkContext;
// import org.apache.spark.api.java.function.VoidFunction;
// import org.apache.spark.sql.Dataset;
// import org.apache.spark.sql.Row;
// import org.apache.spark.sql.hive.HiveContext;
// import org.apache.spark.sql.types.DataTypes;
// import org.apache.spark.sql.types.StructField;
// import org.apache.spark.sql.types.StructType;
// import org.apache.spark.streaming.Duration;
// import org.apache.spark.streaming.api.java.JavaPairInputDStream;
// import org.apache.spark.streaming.api.java.JavaStreamingContext;
// import org.apache.spark.streaming.kafka.KafkaUtils;
//
//
// /***
//  * Spark Stream 拉取kafka的消息
//  */
// public class LoadDataBySparkStream {
//     private static HiveContext hiveContxt;
//     private static SparkConf sparkConf;
//     private static StructType schema;
//     private static JavaSparkContext sparkContext;
//     private static Map<String, String> params;
//     private static Set<String> topics;
//     private static JavaStreamingContext jsc;
//     private static JavaPairInputDStream<String, String> stream;
//
//     public static void main(String[] args) {
//         //最好的方式是从参数传入
//         PropertiesLoad.environment = args[0];
//         //组装Spark启动需要的配置的参数
//         createSparkConf();
//         //组装spark全局的SparkContext
//         createSparkContext();
//         //创建hiveContxt用于向hive写入数据
//         createHiveContext(sparkContext);
//         //创建StreamContext
//         createJavaStreamContext();
//         //创建消费kafka生成Dstream
//         createDStream();
//         //创建解析输入JSON数据的格式
//         createOutputSchema();
//         //数据清洗以及数据存入hive
//         etl(stream);
//         try {
//             jsc.start();
//             jsc.awaitTermination();
//         } catch (Exception e) {
//             System.out.println("Stream Context Exception!");
//         } finally {
//             jsc.stop();
//         }
//     }
//
//     /**
//      * 创建DStream对象,用于从kafka中读取,b并且返回DStream
//      */
//     private static void createDStream() {
//         createParams();
//         stream = KafkaUtils.createDirectStream(jsc, String.class, String.class,
//                 StringDecoder.class, StringDecoder.class, params, topics);
//     }
//
//     /**
//      * 使用SparkConxt对象,创建SparkStreamContx对象用于SparkStream任务调度
//      */
//     private static void createJavaStreamContext() {
//         jsc = new JavaStreamingContext(sparkContext, new Duration(PropertiesLoad.getConfig().getInt("stream.duration.time")));
//     }
//
//     /**
//      * 组装kafkaUtils工具创建DStream所需要的配置文件
//      */
//     private static void createParams() {
//         params = new HashMap<>();
//         params.put("bootstrap.servers", PropertiesLoad.getConfig().getString("bootstrap.servers"));
//         topics = new HashSet<>();
//         topics.add(PropertiesLoad.getConfig().getString("stream.kafka.topics"));
//     }
//
//     /**
//      * 创建SparkContext全局对象
//      */
//     private static void createSparkContext() {
//         sparkContext = new JavaSparkContext(sparkConf);
//     }
//
//     /**
//      * 创建SparkConf配置文件对象
//      */
//     private static void createSparkConf() {
//         sparkConf = new SparkConf()
//                 .set("fs.hdfs.impl", DistributedFileSystem.class.getName())
//                 .set("fs.file.impl", LocalFileSystem.class.getName())
//                 .set("spark.sql.warehouse.dir", PropertiesLoad.getConfig().getString("spark.sql.warehouse.dir"))
//                 .set("dfs.client.use.datanode.hostname", "true")
//                 .set("fs.defaultFS", PropertiesLoad.getConfig().getString("fs.defaultFS"))
//                 .set("ffs.default.name", PropertiesLoad.getConfig().getString("fs.default.name"))
//                 .set("hive.server2.thrift.bind.host", PropertiesLoad.getConfig().getString("hive.server2.thrift.bind.host"))
//                 .set("hive.server2.webui.host", PropertiesLoad.getConfig().getString("hive.server2.webui.host"))
//                 .set("javax.jdo.option.ConnectionURL", PropertiesLoad.getConfig().getString("javax.jdo.option.ConnectionURL"))
//                 .set("hive.metastore.uris", PropertiesLoad.getConfig().getString("hive.metastore.uris"))
//                 .set("mapred.job.tracker", PropertiesLoad.getConfig().getString("mapred.job.tracker"))
//                 .set("dfs.support.append", "true")
//                 .set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
//                 .set("dfs.client.block.write.replace-datanode-on-failure.enable", "true").setAppName("load-data-to-hive")
//                 .setMaster("local[1]");
//     }
//
//     /**
//      * 创建SparkSql中的HiveContx对象
//      *
//      * @param sparkContext
//      */
//     private static void createHiveContext(JavaSparkContext sparkContext) {
//         hiveContxt = new HiveContext(sparkContext);
//     }
//
//     /**
//      * 对DStream流中的每天数据进行遍历
//      *
//      * @param stream
//      */
//
//     private static void etl(JavaPairInputDStream<String, String> stream) {
//         stream.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {
//             @Override
//             public void call(JavaPairRDD<String, String> tuple) throws Exception {
//                 if (tuple.isEmpty()) {
//                     return;
//                 }
//                 Dataset<Row> rowDataset = hiveContxt.jsonRDD(tuple.values(), schema);
//                 writeToHive(rowDataset);
//             }
//         });
//     }
//
//     /**
//      * 创建解析JSON数据的schema结构
//      */
//     private static void createOutputSchema() {
//         List<StructField> outputFields = new ArrayList<>();
//         outputFields.add(DataTypes.createStructField("vccId", DataTypes.StringType, true));
//         outputFields.add(DataTypes.createStructField("clientName", DataTypes.StringType, true));
//         outputFields.add(DataTypes.createStructField("clientPhone", DataTypes.StringType, true));
//         outputFields.add(DataTypes.createStructField("ticketNo", DataTypes.StringType, true));
//         outputFields.add(DataTypes.createStructField("ticketStatus", DataTypes.StringType, true));
//         outputFields.add(DataTypes.createStructField("ticketSource", DataTypes.StringType, true));
//         outputFields.add(DataTypes.createStructField("ticketSourceOrder", DataTypes.StringType, true));
//         outputFields.add(DataTypes.createStructField("ticketPriority", DataTypes.StringType, true));
//         outputFields.add(DataTypes.createStructField("ticketPriorityOrder", DataTypes.IntegerType, true));
//         outputFields.add(DataTypes.createStructField("flowName", DataTypes.StringType, true));
//         outputFields.add(DataTypes.createStructField("flowId", DataTypes.IntegerType, true));
//         outputFields.add(DataTypes.createStructField("ticketTypes", DataTypes.StringType, true));
//         outputFields.add(DataTypes.createStructField("currentNodeName", DataTypes.StringType, true));
//         outputFields.add(DataTypes.createStructField("createUserName", DataTypes.StringType, true));
//         outputFields.add(DataTypes.createStructField("currentUserName", DataTypes.StringType, true));
//         outputFields.add(DataTypes.createStructField("participants", DataTypes.createArrayType(DataTypes.StringType), true));
//         outputFields.add(DataTypes.createStructField("doneUserName", DataTypes.StringType, true));
//         outputFields.add(DataTypes.createStructField("remindNums", DataTypes.IntegerType, true));
//         outputFields.add(DataTypes.createStructField("createTime", DataTypes.StringType, true));
//         outputFields.add(DataTypes.createStructField("updateTime", DataTypes.StringType, true));
//         outputFields.add(DataTypes.createStructField("lastHandleTime", DataTypes.StringType, true));
//         outputFields.add(DataTypes.createStructField("lastSubmitTime", DataTypes.StringType, true));
//         outputFields.add(DataTypes.createStructField("doneTime", DataTypes.StringType, true));
//         outputFields.add(DataTypes.createStructField("sendBackTime", DataTypes.StringType, true));
//         outputFields.add(DataTypes.createStructField("transferTime", DataTypes.StringType, true));
//         outputFields.add(DataTypes.createStructField("nodeAssignTime", DataTypes.createArrayType(DataTypes.StringType), true));
//         outputFields.add(DataTypes.createStructField("nodeStartTime", DataTypes.createArrayType(DataTypes.StringType), true));
//         outputFields.add(DataTypes.createStructField("nodeCreateTime", DataTypes.createArrayType(DataTypes.StringType), true));
//         outputFields.add(DataTypes.createStructField("nodeExpireTime", DataTypes.createArrayType(DataTypes.StringType), true));
//         outputFields.add(DataTypes.createStructField("nodeWaitSecs", DataTypes.createArrayType(DataTypes.StringType), true));
//         outputFields.add(DataTypes.createStructField("nodeHandleSecs", DataTypes.createArrayType(DataTypes.StringType), true));
//         outputFields.add(DataTypes.createStructField("evaluteSendTime", DataTypes.StringType, true));
//         outputFields.add(DataTypes.createStructField("evaluteEndTime", DataTypes.StringType, true));
//         outputFields.add(DataTypes.createStructField("evaluteStar", DataTypes.StringType, true));
//         outputFields.add(DataTypes.createStructField("evaluteTags", DataTypes.createArrayType(DataTypes.StringType), true));
//         outputFields.add(DataTypes.createStructField("totalSecs", DataTypes.IntegerType, true));
//         outputFields.add(DataTypes.createStructField("isFirstDone", DataTypes.IntegerType, true));
//         outputFields.add(DataTypes.createStructField("nodeForm", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true));
//         outputFields.add(DataTypes.createStructField("nodeDynamicField", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true));
//         schema = DataTypes.createStructType(outputFields);
//     }
//
//     /**
//      * 利用Hql语句的insert into table select from table 将解析的DataSet<Row>通过HiveContext存储入hive中
//      *
//      * @param row
//      */
//     private static void writeToHive(Dataset<Row> row) {
//         row.createOrReplaceTempView("tempview");
//         String sql = "insert into " + PropertiesLoad.getConfig().getString("hive.table.ticket") +
//                 " PARTITION(year_month_day='" + new Date() + "') "
//                 + "select vccId as vcc_id, clientName as client_name, clientPhone as client_phone, ticketNo as ticket_no, ticketStatus as ticket_statuts , " +
//                 "ticketSource as ticket_source ,ticketSourceOrder as ticket_source_order, ticketPriority as ticket_priority ,ticketPriorityOrder as ticket_priority_order , flowName flow_name, flowId as flow_id, ticketTypes as ticket_types  , currentNodeName as current_node_name , createUserName as create_user_name , currentUserName as current_user_name , participants , doneUserName as done_user_name, remindNums as remind_nums, createTime as create_time ,updateTime as update_time, lastHandleTime as laste_handle_time , lastSubmitTime as last_submit_time , doneTime as done_time, sendBackTime as send_back_time , transferTime  as transfer_time, nodeAssignTime as node_assign_time , nodeStartTime as node_start_time , nodeCreateTime as node_create_time , nodeExpireTime as node_expire_time, nodeWaitSecs as node_secs , nodeHandleSecs as node_handle_secs , evaluteSendTime as evalute_send_time , evaluteEndTime as evalute_end_time, evaluteStar as evalute_star , evaluteTags as evalute_tags , totalSecs as total_secs , isFirstDone as is_first_secs , nodeForm as node_form , nodeDynamicField as node_dynamic_fie from tempview";
//         long start = System.currentTimeMillis();
//         hiveContxt.sql(sql);
//         long end = System.currentTimeMillis();
//         System.out.println("insert into hive Cost Time   " + (end - start) + "      ones time size is    " + row.count());
//     }
// }
