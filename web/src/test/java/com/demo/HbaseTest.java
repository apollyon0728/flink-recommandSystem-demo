package com.demo;


import com.demo.client.HbaseClient;
import com.demo.service.UserScoreService;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
@Rollback
public class HbaseTest {

    @Autowired
    UserScoreService userScoreService;

    @Test
    public void testHbase() throws IOException {
        userScoreService.calUserScore("1");
    }


    @Test
    public void testHbaseClient() throws IOException {
        String data = HbaseClient.getData("user", "1", "color", "red");
        System.out.println(data);
    }

    @Test
    public void testCreateTable() throws IOException {
        HbaseClient.createTable("test", new String[]{"log", "test01", "test02"});
    }

    @Test
    public void testDeleteTable() throws IOException {
        HbaseClient.deleteTable("test");
    }

    @Test
    public void testAllKey() throws IOException {
        Scan scan = new Scan();
        Table table = HbaseClient.conn.getTable(TableName.valueOf("test"));
        ResultScanner scanner = table.getScanner(scan);
        for (Result r : scanner) {
            System.out.println(">>> Result : " + new String(r.getRow()));
        }
    }

    @Test
    public void putDateTest() throws Exception {
        String rowKey = "row-key-000-111";
        for (int usrId =0; usrId < 10; usrId++) {
            HbaseClient.putData("test",rowKey,"log","userid", String.valueOf(usrId));
        }

    }
}

/*
 * 【例子参考】
 * https://github.com/apollyon0728/flink-recommandSystem-demo
 *
 * https://blog.csdn.net/weixin_41407399/article/details/79943760
 */
