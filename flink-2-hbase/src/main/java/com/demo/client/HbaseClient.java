package com.demo.client;

import com.demo.domain.LogInfo;
import com.demo.domain.User;
import com.demo.util.Property;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.*;

public class HbaseClient {
    private static Admin admin;
    public static Connection conn;

    static {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir", Property.getStrValue("hbase.rootdir"));
        conf.set("hbase.zookeeper.quorum", Property.getStrValue("hbase.zookeeper.quorum"));
        conf.set("hbase.client.scanner.timeout.period", Property.getStrValue("hbase.client.scanner.timeout.period"));
        conf.set("hbase.rpc.timeout", Property.getStrValue("hbase.rpc.timeout"));
        try {
            System.setProperty("hadoop.home.dir", "G:\\download\\hadoop2.6_Win_x64-master");
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void createTable(String tableName, String... columnFamilies) throws IOException {
        TableName tablename = TableName.valueOf(tableName);
        if (admin.tableExists(tablename)) {
            System.out.println("Table Exists");
        } else {
            System.out.println("Start create table");
            HTableDescriptor tableDescriptor = new HTableDescriptor(tablename);
            for (String columnFamily : columnFamilies) {
                HTableDescriptor column = tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
            }
            admin.createTable(tableDescriptor);
            System.out.println("Create Table success");
        }
    }

    /**
     * 获取一列获取一行数据
     *
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param column
     * @return
     * @throws IOException
     */
    public static String getData(String tableName, String rowKey, String familyName, String column) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        byte[] row = Bytes.toBytes(rowKey);
        Get get = new Get(row);
        Result result = table.get(get);
        byte[] resultValue = result.getValue(familyName.getBytes(), column.getBytes());
        if (null == resultValue) {
            return null;
        }
        return new String(resultValue);
    }


    /**
     * 获取一行的所有数据 并且排序
     * 【非标准方法，针对某个表】 因为代码中的转换方法
     *
     * @param tableName 表名
     * @param rowKey    列名
     * @throws IOException
     */
    public static List<Map.Entry> getRow(String tableName, String rowKey) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        byte[] row = Bytes.toBytes(rowKey);
        Get get = new Get(row);
        Result r = table.get(get);

        HashMap<String, Double> rst = new HashMap<>();

        for (Cell cell : r.listCells()) {
            String key = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
            String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());

            // 【非标准方法，针对某个表】
            rst.put(key, new Double(value));
        }

        List<Map.Entry> ans = new ArrayList<>();
        ans.addAll(rst.entrySet());

        Collections.sort(ans, (m1, m2) -> new Double((Double) m1.getValue() - (Double) m2.getValue()).intValue());

        return ans;
    }

    /**
     * 向对应列添加数据
     *
     * @param tableName  表名
     * @param rowKey     行号
     * @param familyName 列族名
     * @param column     列名
     * @param data       数据
     * @throws Exception
     */
    public static void putData(String tableName, String rowKey, String familyName, String column, String data) throws Exception {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(familyName.getBytes(), column.getBytes(), data.getBytes());
        table.put(put);
    }

    /**
     * 将该单元格加1
     *
     * @param tableName  表名
     * @param rowKey     行号
     * @param familyName 列族名
     * @param column     列名
     * @throws Exception
     */
    public static void increaseColumn(String tableName, String rowKey, String familyName, String column) throws Exception {
        String val = getData(tableName, rowKey, familyName, column);
        int res = 1;
        if (val != null) {
            res = Integer.valueOf(val) + 1;
        }
        putData(tableName, rowKey, familyName, column, String.valueOf(res));
    }

    public static void main(String[] args) throws IOException {
        List<Map.Entry> ps = HbaseClient.getRow("ps", "1");
        ps.forEach(System.out::println);
    }


    /**
     * 取出表中所有的key
     *
     * @param tableName
     * @return
     */
    public static List<String> getAllKey(String tableName) throws IOException {
        List<String> keys = new ArrayList<>();
        Scan scan = new Scan();
        Table table = HbaseClient.conn.getTable(TableName.valueOf(tableName));
        ResultScanner scanner = table.getScanner(scan);
        for (Result r : scanner) {
            keys.add(new String(r.getRow()));
        }
        return keys;
    }

    /**
     * 删除表
     *
     * @param tableName
     * @throws IOException
     */
    public static void deleteTable(String tableName) throws IOException {
        TableName tablename = TableName.valueOf(tableName);
        if (admin.tableExists(tablename)) {
            System.out.println("Table Exists");

            // org.apache.hadoop.hbase.TableNotDisabledException
            // 删除前需要先禁用
            admin.disableTable(tablename);

            admin.deleteTable(tablename);

            System.out.println(tablename + " Table is deleted");
        }
    }

    //获取原始数据
    public static void getNoDealData(String tableName) {
        try {
            Table table = conn.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                System.out.println("scan:  " + result);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void insertUserDataTest(String tableName, User user) throws IOException {
        TableName tablename = TableName.valueOf(tableName);
        Put put = new Put(("user-" + user.getId()).getBytes());
        //参数：1.列族名  2.列名  3.值
        put.addColumn("information".getBytes(), "username".getBytes(), user.getUserName().getBytes());
        put.addColumn("information".getBytes(), "age".getBytes(), String.valueOf(user.getAge()).getBytes());
//        put.addColumn("information".getBytes(), "gender".getBytes(), user.getGender().getBytes()) ;
        put.addColumn("contact".getBytes(), "phone".getBytes(), user.getPhone().getBytes());
//        put.addColumn("contact".getBytes(), "email".getBytes(), user.getEmail().getBytes());
        Table table = HbaseClient.conn.getTable(tablename);
        table.put(put);
    }

    public static void insertLogInfoDataTest(String tableName, LogInfo logInfo) throws IOException {
        TableName tablename = TableName.valueOf(tableName);
        String timeStr = String.valueOf(new Date().getTime());
        // rowKey 设置为erp + time
        Put put = new Put((logInfo.getErp().concat(timeStr)).getBytes());
        //参数：1.列族名  2.列名  3.值
        put.addColumn("info".getBytes(), "id".getBytes(), logInfo.getId().getBytes());
        put.addColumn("info".getBytes(), "erp".getBytes(), logInfo.getErp().getBytes());
        put.addColumn("msg".getBytes(), "status".getBytes(), logInfo.getStatus().getBytes());
        put.addColumn("msg".getBytes(), "msg".getBytes(), logInfo.getMsg().getBytes());
        put.addColumn("msg".getBytes(), "createTime".getBytes(), logInfo.getCreateTime().getBytes());
        put.addColumn("msg".getBytes(), "time".getBytes(), timeStr.getBytes());
        Table table = HbaseClient.conn.getTable(tablename);
        table.put(put);
    }

    public static User getUserRow(String tableName, String rowKey) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        byte[] row = Bytes.toBytes(rowKey);
        Get get = new Get(row);
        Result r = table.get(get);

        User user = processUser(r);
        if (user != null) return user;
        return null;
    }

    @Nullable
    private static User processUser(Result r) {
        HashMap<String, String> rst = new HashMap<>();

        for (Cell cell : r.listCells()) {
            String key = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
            String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            // 【非标准方法，针对某个表】
            rst.put(key, value);
        }

        try {
            User user = (User) com.demo.util.MapUtils.transMap(User.class, rst);
            return user;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 范围查询结果遍历
     * https://www.cnblogs.com/similarface/p/5799460.html
     * @param tableName
     * @param queryRowKey
     * @return
     * @throws IOException
     */
    public static List<User> getUserList(String tableName, String queryRowKey) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        PageFilter pf = new PageFilter(2L);
        Scan scan = new Scan();
        scan.setFilter(pf);
        scan.setStartRow(Bytes.toBytes(queryRowKey));

        ResultScanner scanner = table.getScanner(scan);
        List<User> userList = new ArrayList<>();
        for (Result res : scanner) {
            User user = processUser(res);
            userList.add(user);
        }
        return userList;
    }

    public static List<LogInfo> getLogList(String tableName, String queryRowKey) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        PageFilter pf = new PageFilter(10L);
        Scan scan = new Scan();
        scan.setFilter(pf);
        scan.setStartRow(Bytes.toBytes(queryRowKey));

        ResultScanner scanner = table.getScanner(scan);
        List<LogInfo> logInfoList = new ArrayList<>();
        for (Result res : scanner) {
            LogInfo logInfo = processLogInfo(res);
            logInfoList.add(logInfo);
        }
        return logInfoList;
    }

    /**
     * 模糊查询
     * https://blog.csdn.net/ZYJ_2012/article/details/52222572?utm_medium=distribute.pc_aggpage_search_result.none-task-blog-2~all~first_rank_v2~rank_v25-2-52222572.nonecase&utm_term=hbase%20%E6%8C%89%E7%85%A7%E6%97%B6%E9%97%B4%E8%8C%83%E5%9B%B4%E6%9F%A5%E8%AF%A2%E6%95%B0%E6%8D%AE&spm=1000.2123.3001.4430
     * @param tableName
     * @param queryRowKey
     * @param sTime
     * @param eTime
     * @return
     * @throws IOException
     *
     * 比较运算符 CompareFilter.CompareOp
     * EQUAL                                  相等
     * GREATER                              大于
     * GREATER_OR_EQUAL           大于等于
     * LESS                                      小于
     * LESS_OR_EQUAL                  小于等于
     * NOT_EQUAL                        不等于
     */
    public static List<LogInfo> getLogListByCreatTime(String tableName, String queryRowKey, String sTime, String eTime ) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(queryRowKey));

        FilterList filterList=new FilterList();
        Filter sTimeFilter = new SingleColumnValueFilter(Bytes.toBytes("msg"),Bytes.toBytes("createTime"), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(sTime));
        filterList.addFilter(sTimeFilter);
        Filter eTimeFilter = new SingleColumnValueFilter(Bytes.toBytes("msg"),Bytes.toBytes("createTime"), CompareFilter.CompareOp.LESS_OR_EQUAL,Bytes.toBytes(eTime));
        filterList.addFilter(eTimeFilter);
        scan.setFilter(filterList);


        ResultScanner scanner = table.getScanner(scan);
        List<LogInfo> logInfoList = new ArrayList<>();
        for (Result res : scanner) {
            LogInfo logInfo = processLogInfo(res);
            logInfoList.add(logInfo);
        }
        return logInfoList;
    }

    @Nullable
    private static LogInfo processLogInfo(Result r) {
        HashMap<String, String> rst = new HashMap<>();
        for (Cell cell : r.listCells()) {
            String key = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
            String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            rst.put(key, value);
        }
        try {
            LogInfo logInfo = (LogInfo) com.demo.util.MapUtils.transMap(LogInfo.class, rst);
            return logInfo;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}
