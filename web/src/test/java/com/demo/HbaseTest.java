package com.demo;


import com.alibaba.fastjson.JSONObject;
import com.demo.client.HbaseClient;
import com.demo.domain.User;
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
import java.util.Date;
import java.util.List;
import java.util.Map;

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

    /**
     * 建表
     * @throws IOException
     */
    @Test
    public void testCreateTable() throws IOException {
        // 创建test表
        HbaseClient.createTable("test", new String[]{"log", "test01", "test02"});

        // 创建user_table表
        HbaseClient.createTable("user_table", new String[]{ "information", "contact" });
    }

    /**
     * 删除表
     * @throws IOException
     */
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

    /**
     * 插入数据
     * @throws Exception
     */
    @Test
    public void putDateTest() throws Exception {
        String rowKey = "row-key-000-111";
        for (int usrId =0; usrId < 10; usrId++) {
            HbaseClient.putData("test",rowKey,"log","userid", String.valueOf(usrId));
        }
    }


    /**
     * 获取一列获取一行数据
     * @throws IOException
     */
    @Test
    public void getDataTest() throws IOException {
        String data = HbaseClient.getData("test", "row-key-000-111", "log", "userid");
        System.out.println(">>>>>> getHbaseData: " + data);
    }

    /**
     * 获取一行的所有数据 并且排序
     * @throws IOException
     */
    @Test
    public void getRowTest() throws IOException {
        List<Map.Entry> map = HbaseClient.getRow("test", "row-key-000-111");
        System.out.println(">>>>>> getHbaseData: " + JSONObject.toJSONString(map));
    }

    /**
     * 取出表中所有的key
     * >>>>>> getAllKeyTest: ["row-key-000-111"]
     * @throws IOException
     */
    @Test
    public void getAllKeyTest() throws IOException {
        List<String> result = HbaseClient.getAllKey("test");
        System.out.println(">>>>>> getAllKeyTest test: " + JSONObject.toJSONString(result));

        List<String> resultUser = HbaseClient.getAllKey("user_table");
        System.out.println(">>>>>> getAllKeyTest user_table: " + JSONObject.toJSONString(resultUser));
    }


    @Test
    public void testCreateUserTable() throws IOException {
        // 创建user_table表
        HbaseClient.createTable("user_table", new String[]{"information", "contact"});
    }


    /**
     * 插入数据
     * https://blog.csdn.net/m0_38075425/article/details/81287836
     */
    @Test
    public void insertDataTest() throws IOException {
        for (int userId = 0; userId< 10000000; userId++) {
            User user = new User();
            user.setUserName("name".concat(String.valueOf(userId)));
            user.setId(String.valueOf(userId));
            user.setAge(String.valueOf(10+userId));
            user.setPhone("133".concat("-").concat(String.valueOf(userId)).concat("-").concat(String.valueOf(new Date().getTime())));
            HbaseClient.insertUserDataTest("user_table", user);
        }
    }


    @Test
    public void getUserRowTest() throws IOException {
        User user = HbaseClient.getUserRow("user_table", "user-100");
        System.out.println(">>>>>> getUserRowTest user: " + JSONObject.toJSONString(user));

        String phone = HbaseClient.getData("user_table", "user-10000", "contact", "phone");
        System.out.println(">>>>>> getUserRowTest user#phone: " + phone);
    }

    @Test
    public void getUserListTest() throws IOException {
        List<User> userList = HbaseClient.getUserList("user_table", "user-");
        System.out.println(">>>>>> getUserListTest userList: " + JSONObject.toJSONString(userList));
    }


}

/*
 *  测试环境
 *  http://hr-hbase-test-afa55fbe:11069/rs-status#baseStats
 *
 * GIT项目
 * https://github.com/apollyon0728/flink-recommandSystem-demo
 *
 * rowKey设计
 * https://blog.csdn.net/u012834750/article/details/81708669?utm_medium=distribute.pc_aggpage_search_result.none-task-blog-2~all~first_rank_v2~rank_v25-1-81708669.nonecase&utm_term=hbase%20rowkey%E9%87%8D%E5%A4%8D&spm=1000.2123.3001.4430
 *
 * 深入浅出HBASE
 * https://developer.ibm.com/zh/articles/ba-cn-bigdata-hbase/
 *
 * 【例子参考】
 * https://blog.csdn.net/weixin_41407399/article/details/79943760
 *
 * 范围查询
 * https://blog.csdn.net/kangkangwanwan/article/details/89332536
 *
 * 范围查询结果遍历
 * https://www.cnblogs.com/similarface/p/5799460.html
 *
 * HBase的java操作，
 * 最新API https://blog.csdn.net/m0_38075425/article/details/81287836
 * https://github.com/apollyon0728/God-Of-BigData/blob/master/%E5%A4%A7%E6%95%B0%E6%8D%AE%E6%A1%86%E6%9E%B6%E5%AD%A6%E4%B9%A0/Hbase_Java_API.md
 *
 *
 *
 * 使用Docker部署Flink大数据项目
 * https://xinze.fun/2019/11/19/%E4%BD%BF%E7%94%A8Docker%E9%83%A8%E7%BD%B2Flink%E5%A4%A7%E6%95%B0%E6%8D%AE%E9%A1%B9%E7%9B%AE/
 *
 * HBase命令行
 * https://blog.csdn.net/vbirdbest/article/details/88236575
 *
 *
 * 二级索引
 * https://www.cnblogs.com/kxdblog/p/4328699.html
 */
