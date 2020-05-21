package com.canal;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.protocol.Message;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.checkerframework.checker.units.qual.C;

import static com.canal.CanalConfig.CANAL_ADDRESS;
import static com.canal.CanalConfig.DESTINATION;
import static com.canal.CanalConfig.FILTER;
import static com.canal.CanalConfig.PORT;

/**
 * @author chenpeibin
 * @version 1.0
 * @date 2020/5/6 10:27
 */
@Slf4j
public class CanalClient {
    private static Queue<String> SQL_QUEUE = new ConcurrentLinkedQueue<>();

    // public static void main(String args[]) {
    //
    //     CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(CANAL_ADDRESS,
    //             PORT), DESTINATION, "", "");
    //     int batchSize = 1000;
    //     try {
    //         connector.connect();
    //         connector.subscribe(FILTER);
    //         connector.rollback();
    //         try {
    //             while (true) {
    //                 //尝试从master那边拉去数据batchSize条记录，有多少取多少
    //                 Message message = connector.getWithoutAck(batchSize);
    //                 long batchId = message.getId();
    //                 int size = message.getEntries().size();
    //                 if (batchId == -1 || size == 0) {
    //                     Thread.sleep(1000);
    //                 } else {
    //                     dataHandle(message.getEntries());
    //                 }
    //                 //手动确认
    //                 // connector.ack(batchId);
    //
    //                 //当队列里面堆积的sql大于一定数值的时候就模拟执行
    //                 if (SQL_QUEUE.size() >= 10) {
    //                     executeQueueSql();
    //                 }
    //                 Thread.sleep(50000L);
    //             }
    //         } catch (InterruptedException e) {
    //             log.info("e:{}", e);
    //         } catch (InvalidProtocolBufferException e) {
    //             log.info("e:{}", e);
    //         }
    //     } finally {
    //         connector.disconnect();
    //     }
    // }


    /**
     * 模拟执行队列里面的sql语句
     */
    public static void executeQueueSql() {
        int size = SQL_QUEUE.size();
        for (int i = 0; i < size; i++) {
            String sql = SQL_QUEUE.poll();
            log.info("[sql]----> " + sql);
        }
    }

    /**
     * 数据处理
     *
     * @param entrys
     */
    private static void dataHandle(List<Entry> entrys) throws InvalidProtocolBufferException {
        for (Entry entry : entrys) {
            if (EntryType.ROWDATA == entry.getEntryType()) {
                RowChange rowChange = RowChange.parseFrom(entry.getStoreValue());
                EventType eventType = rowChange.getEventType();
                if (eventType == EventType.DELETE) {
                    saveDeleteSql(entry);
                } else if (eventType == EventType.UPDATE) {
                    saveUpdateSql(entry);
                } else if (eventType == EventType.INSERT) {
                    saveInsertSql(entry);
                }
            }
        }
    }

    /**
     * 保存更新语句
     *
     * @param entry
     */
    private static void saveUpdateSql(Entry entry) {
        try {
            RowChange rowChange = RowChange.parseFrom(entry.getStoreValue());
            List<RowData> rowDatasList = rowChange.getRowDatasList();
            for (RowData rowData : rowDatasList) {
                List<Column> newColumnList = rowData.getAfterColumnsList();
                StringBuffer sql = new StringBuffer("update " + entry.getHeader().getSchemaName() + "." + entry.getHeader().getTableName() + " set ");
                for (int i = 0; i < newColumnList.size(); i++) {
                    sql.append(" " + newColumnList.get(i).getName()
                            + " = '" + newColumnList.get(i).getValue() + "'");
                    if (i != newColumnList.size() - 1) {
                        sql.append(",");
                    }
                }
                sql.append(" where ");
                List<Column> oldColumnList = rowData.getBeforeColumnsList();
                for (Column column : oldColumnList) {
                    if (column.getIsKey()) {
                        //暂时只支持单一主键
                        sql.append(column.getName() + "=" + column.getValue());
                        break;
                    }
                }
                SQL_QUEUE.add(sql.toString());
            }
        } catch (InvalidProtocolBufferException e) {
            log.info("e:{}", e);
        }
    }

    /**
     * 保存删除语句
     *
     * @param entry
     */
    private static void saveDeleteSql(Entry entry) {
        try {
            RowChange rowChange = RowChange.parseFrom(entry.getStoreValue());
            List<RowData> rowDatasList = rowChange.getRowDatasList();
            for (RowData rowData : rowDatasList) {
                List<Column> columnList = rowData.getBeforeColumnsList();
                StringBuffer sql = new StringBuffer("delete from " + entry.getHeader().getSchemaName() + "." + entry.getHeader().getTableName() + " where ");
                for (Column column : columnList) {
                    if (column.getIsKey()) {
                        //暂时只支持单一主键
                        sql.append(column.getName() + "=" + column.getValue());
                        break;
                    }
                }
                SQL_QUEUE.add(sql.toString());
            }
        } catch (InvalidProtocolBufferException e) {
            log.info("e:{}", e);
        }
    }

    /**
     * 保存插入语句
     *
     * @param entry
     */
    private static void saveInsertSql(Entry entry) {
        try {
            RowChange rowChange = RowChange.parseFrom(entry.getStoreValue());
            List<RowData> rowDatasList = rowChange.getRowDatasList();
            for (RowData rowData : rowDatasList) {
                List<Column> columnList = rowData.getAfterColumnsList();
                StringBuffer sql = new StringBuffer("insert into " + entry.getHeader().getSchemaName() + "." + entry.getHeader().getTableName() + " (");
                for (int i = 0; i < columnList.size(); i++) {
                    sql.append(columnList.get(i).getName());
                    if (i != columnList.size() - 1) {
                        sql.append(",");
                    }
                }
                sql.append(") VALUES (");
                for (int i = 0; i < columnList.size(); i++) {
                    sql.append("'" + columnList.get(i).getValue() + "'");
                    if (i != columnList.size() - 1) {
                        sql.append(",");
                    }
                }
                sql.append(")");
                SQL_QUEUE.add(sql.toString());
            }
        } catch (InvalidProtocolBufferException e) {
            log.info("e:{}", e);
        }
    }

    private static Object saveInsertEntity(Entry entry){
        try {
            RowChange rowChange = RowChange.parseFrom(entry.getStoreValue());
            List<RowData> rowDataList = rowChange.getRowDatasList();
            Map<String,Object> map = Maps.newHashMap();
            for (RowData rowData : rowDataList) {
                List<Column> columnList = rowData.getAfterColumnsList();
                for (Column column : columnList) {
                    String name = column.getName();
                    String value = column.getValue();
                    String key = camelName(name);
                    map.put(key,value);
                }
                //库名
                String schemaName = entry.getHeader().getSchemaName();
                //表名
                String tableName = entry.getHeader().getTableName();


            }
        } catch (InvalidProtocolBufferException e) {
            log.info("e:{}", e);
        }
        return null;
    }

    public static String camelName(String str){
        StringBuilder builder = new StringBuilder();
        List<String> strings = Arrays.asList(str.split("_"));
        for(int i = 0 ; i < strings.size(); i++){
            if (i == 0){
                builder.append(strings.get(i));
            }else {
                builder.append(StringUtils.capitalize(strings.get(i)));
            }

        }
        return builder.toString();
    }

    public static void main(String[] args) {
        CanalClient canalClient = new CanalClient();
        System.out.println(camelName("tenant_id"));
    }
}
