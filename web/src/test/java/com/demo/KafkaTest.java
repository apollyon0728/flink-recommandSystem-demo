package com.demo;


import com.alibaba.fastjson.JSON;
import com.demo.domain.Order;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.Random;


/**
 * MY: kafka 测试
 *
 * Java操作Kafka创建主题、生产者、消费者
 * https://www.cnblogs.com/fhblikesky/p/13692669.html
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class KafkaTest {

    //kafka服务地址
    private static String SERVERS_CONFIG_VAL = "localhost:9092";
    private  Properties props;

    @Before
    public void init() {
        props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS_CONFIG_VAL); //kafka服务地址
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }

    /**
     * 创建主题
     */
    @Test
    public void createTopicTest() {
        /*Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.25.132:9092");  //kafka服务地址
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());*/

        AdminClient client = KafkaAdminClient.create(props);//创建操作客户端
        //创建名称为test1的topic，有5个分区
//        NewTopic topic = new NewTopic("test1", 5, (short) 1);
        NewTopic topic = new NewTopic("topic_topN", 5, (short) 1);
        client.createTopics(Arrays.asList(topic));
        client.close();//关闭
    }

    /**
     * kafka 生产消息
     */
    @Test
    public void producerTest() {
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        //异步发送20条消息
        for (int i = 1; i <= 20; i++){
            ProducerRecord<String, String> record = new ProducerRecord<>("test1", "key" + i, "message" + i);
            producer.send(record);
        }

        producer.close();
    }


    @Test
    public void consumerTest() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS_CONFIG_VAL);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group-1");//groupid相同的属于同一个消费者组
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);//自动提交offset
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //消费test1主题
        consumer.subscribe(Arrays.asList("test1"));
        while (true){
            System.out.println("consumer is polling");
            //5秒等待
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(String.format("offset=%d，key=%s，value=%s",
                        record.offset(), record.key(), record.value()));
            }
            //同步提交，失败会重试
            consumer.commitSync();
            //异步提交，失败不会重试
            //consumer.commitAsync();
        }
    }


    @Test
    public void producerOrderTest() throws InterruptedException {
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        int j = 1;
        //异步发送20条消息
        for (int i = 1; i <= 10; i++){
            String orderId;
            String areaId;
            if (i % 2 == 0) {
                orderId  = String.valueOf(new Random().nextInt(20) + 1);
                areaId = "110103";
            } else {
                orderId  = String.valueOf(new Random().nextInt(20) + 1);
                areaId = "110106";
            }
            int amount = new Random().nextInt(100) + 1;

            Order order = new Order(orderId, new Date().getTime(), "gdsId".concat(String.valueOf(i)), (double) amount, areaId);

            ProducerRecord<String, String> record = new ProducerRecord<>("topic_topN", "key" + j, JSON.toJSONString(order));
            producer.send(record);

            j++;

            if (i == 10) {
                i = 1;
                Thread.sleep(5000);
            }
        }

        producer.close();
    }
}


/*
生产者
producer.send(record, new Callback() {
    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e == null){
            System.out.println("success：" + recordMetadata.offset());
        }
        else{
            e.printStackTrace();
        }
    }
});
同步发送：消息发送后，会堵塞当前线程直到收到ack为止，才继续发送

//在后面加上get()即可
producer.send(record).get();

 */