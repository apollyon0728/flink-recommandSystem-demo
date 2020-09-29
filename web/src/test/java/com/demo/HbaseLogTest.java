package com.demo;


import com.alibaba.fastjson.JSONObject;
import com.demo.client.HbaseClient;
import com.demo.domain.LogInfo;
import com.demo.enums.DateStyle;
import com.demo.util.DateUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.util.Date;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
@Rollback
public class HbaseLogTest {


    /**
     * 建表
     * @throws IOException
     */
    @Test
    public void testCreateTable() throws IOException {
        // 创建user_table表
        HbaseClient.createTable("log_table", new String[]{ "info", "msg" });
    }

    /**
     * 删除表
     * @throws IOException
     */
    @Test
    public void testDeleteTable() throws IOException {
        HbaseClient.deleteTable("log_table");
    }

    @Test
    public void testAllKey() throws IOException {
        Scan scan = new Scan();
        Table table = HbaseClient.conn.getTable(TableName.valueOf("log_table"));
        ResultScanner scanner = table.getScanner(scan);
        for (Result r : scanner) {
            System.out.println(">>> Result : " + new String(r.getRow()));
        }
    }

    /**
     * 取出表中所有的key
     * @throws IOException
     */
    @Test
    public void getAllKeyTest() throws IOException {
        List<String> resultUser = HbaseClient.getAllKey("log_table");
        System.out.println(">>>>>> getAllKeyTest log_table: " + JSONObject.toJSONString(resultUser));
    }


    /**
     * 插入数据，设计时间字段，后续可用时间范围查询
     * https://blog.csdn.net/m0_38075425/article/details/81287836
     * https://blog.csdn.net/ZYJ_2012/article/details/52222572?utm_medium=distribute.pc_aggpage_search_result.none-task-blog-2~all~first_rank_v2~rank_v25-2-52222572.nonecase&utm_term=hbase%20%E6%8C%89%E7%85%A7%E6%97%B6%E9%97%B4%E8%8C%83%E5%9B%B4%E6%9F%A5%E8%AF%A2%E6%95%B0%E6%8D%AE&spm=1000.2123.3001.4430
     */
    @Test
    public void insertLogInfoTest() throws IOException {
//        String erp = "zhangsan";

        // 2020-09-29 18:17:56
//        String erp = "lisi";

        // 2020-09-29 18:24:08
        String erp = "wangwu";
        for (int index = 0; index< 100000; index++) {
            String timeStr = String.valueOf(new Date().getTime());
            LogInfo logInfo = new LogInfo();
            // CF 为 info
            logInfo.setId(String.valueOf(index));
            // erp + time 作为rowKey
            logInfo.setErp(erp);

            // CF 为 msg
            if (index % 2 == 0) {
                logInfo.setStatus(String.valueOf(0));
            } else {
                logInfo.setStatus(String.valueOf(1));
            }
            logInfo.setMsg("msg".concat("-").concat(String.valueOf(index)).concat("-").concat(timeStr));
            logInfo.setTime(timeStr);
            String createTime = DateUtil.dateToString(new Date(), DateStyle.YYYY_MM_DD_HH_MM_SS);
            logInfo.setCreateTime(createTime);
            HbaseClient.insertLogInfoDataTest("log_table", logInfo);

            System.out.println(">>>>>> insertLogInfoTest logInfo: " + JSONObject.toJSONString(logInfo));
        }
    }


    @Test
    public void getLogListTest() throws IOException {
        List<LogInfo> logInfoList = HbaseClient.getLogList("log_table", "zhangsan-");
        System.out.println(">>>>>> getUserListTest getUserListTest: " + JSONObject.toJSONString(logInfoList));
    }

    /**
     * 按照时间范围查询数据
     */
    @Test
    public void getLogListTestByCreatTime() throws IOException {
        // 在这个时间范围写入了数据，create_time取了系统时间
        String sTime = "2020-09-29 16:46:54";
        String eTime = "2020-09-29 16:46:55";
        List<LogInfo> logInfoList = HbaseClient.getLogListByCreatTime("log_table", "zhangsan-", sTime, eTime);
        System.out.println(">>>>>> getUserListTest getUserListTest: " + JSONObject.toJSONString(logInfoList));
    }

}




/*
 * git token
 * 1e40c9b3dd907cee4f99ecd966e581d9488b9961
 *
 * GIT项目
 * https://github.com/apollyon0728/flink-recommandSystem-demo
 *
 * 【Hbase技术详细学习笔记】，还不错
 * https://www.jianshu.com/p/569106a3008f
 *
 * 【HBase深入浅出】 还不错
 * https://developer.ibm.com/zh/articles/ba-cn-bigdata-hbase/
 *
 *
 * HBase更像是数据存储，支持PB级别数据
 *
 * 大数据
 * https://github.com/apollyon0728/God-Of-BigData#%E4%BA%94hbase
 */
