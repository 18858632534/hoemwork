package com.shun.lagou.mr.module_4.work_2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LinkedHBase {
    private static Logger logger = LoggerFactory.getLogger(LinkedHBase.class);
    private static Configuration conf = null;
    private static Connection conn = null;
    private static HBaseAdmin admin = null;
    private static String tableName = "user";
    private static String familyName = "friends";
    private static String deleteColumn = "deleteId";

    public static void init() {
        try {
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "linux121,linux122");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void createTable() throws IOException {
        admin = (HBaseAdmin) conn.getAdmin();
        //创建表描述器
        HTableDescriptor teacher = new HTableDescriptor(TableName.valueOf(tableName));
        //设置列族描述器
        teacher.addFamily(new HColumnDescriptor(familyName));
        //执行创建操作
        admin.createTable(teacher);
        logger.info(String.format("create %s success !!!", tableName));
    }

    private static void putData() throws IOException {
        //获取一个表对象
        Table t = conn.getTable(TableName.valueOf(tableName));
        List<Put> putList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            //设定rowkey
            Put put = new Put(Bytes.toBytes(String.valueOf(i)));
            for (int j = 0; j < 10; j++) {
                //列族，列，valu
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(String.valueOf(j)), Bytes.toBytes(String.valueOf(j)));
            }
            putList.add(put);
        }
        //执行插入
        t.put(putList);
        // 关闭table对象m
        t.close();
    }

    private static void deleteFriend(int key, int column) throws IOException {
        //获取一个表对象
        Table t = conn.getTable(TableName.valueOf(tableName));
        byte[] rowKey = Bytes.toBytes(String.valueOf(key));
        Get get = new Get(rowKey);
        get.addFamily(Bytes.toBytes(familyName));
        Result result = t.get(get);
        //设定rowkey
        Put put = new Put(rowKey);
        //列族，列，value
        for (int i = 0; i < 10; i++) {
            if (i != column) {
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(String.valueOf(i)), result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(String.valueOf(i))));
            }
        }
        // 增加一列用来说明删除了哪个好友,联动删除就无需遍历了
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(deleteColumn), result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(String.valueOf(column))));
        //执行插入
        t.put(put);
        // 关闭table对象
        t.close();
    }


    public static void main(String[] args) {
        init();
        logger.info(String.valueOf(conn == null));
    }
}

