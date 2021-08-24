package com.shun.lagou.mr.module_4.work_1;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.dbcp2.BasicDataSourceFactory;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class DynamicConnectionPool {

    private static ZkClient zkClient = null;
    private static volatile DataSource dataSource = null;
    private static final String path = "/servers/mysql.property";

    static  {
        zkClient = new ZkClient("linux121:2181,linux122:2181");
        zkClient.subscribeDataChanges(path, new IZkDataListener() {
            @Override
            public void handleDataChange(String s, Object o) throws Exception {
                dataSource = initDataSource(o);
            }

            @Override
            public void handleDataDeleted(String s) throws Exception {

            }
        });
        try {
            dataSource = initDataSource(zkClient.readData(path));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * init DBCP connection pool
     * @param o
     * @return
     * @throws Exception
     */
    private static DataSource initDataSource(Object o) throws Exception {
        String str = (String) o;
        Properties properties = new Properties();
        properties.load(new ByteArrayInputStream(str.getBytes()));
        return BasicDataSourceFactory.createDataSource(properties);
    }

    public static Connection getConnection() {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return connection;
    }
}
