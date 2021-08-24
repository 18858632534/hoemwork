package com.shun.lagou.mr.module_4.work_2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class MyProcessor extends BaseRegionObserver {
    private  Connection conn = null;
    private  HBaseAdmin admin = null;
    private  String tableName = "user";
    private  String familyName = "friends";
    private  String deleteColumn = "deleteId";

    public MyProcessor() {
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "linux121,linux122");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void deleteFriend(byte[] rowKey, byte[] column) throws IOException {
        //获取一个表对象
        Table t = conn.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey);
        get.addFamily(Bytes.toBytes(familyName));
        Result result = t.get(get);
        if (result.getValue(Bytes.toBytes(familyName), column).length > 0) {
            //设定rowkey
            Put put = new Put(rowKey);
            for (int i = 0; i < 10; i++) {
                if (i != Integer.valueOf(String.valueOf(column))) {
                    //列族，列，value
                    put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(String.valueOf(i)), result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(String.valueOf(i))));
                }
            }
            //执行插入
            t.put(put);
        }
        // 关闭table对象
        t.close();
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {

        byte[] rowKey = put.getRow();
        // 要联动删除的rowKey
        byte[] deleteRow = put.getAttribute(deleteColumn);
        // 这种表结构只能遍历列来一一删除
        deleteFriend(deleteRow, rowKey);
    }
}
