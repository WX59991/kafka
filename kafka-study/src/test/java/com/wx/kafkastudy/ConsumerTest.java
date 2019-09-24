package com.wx.kafkastudy;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collections;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ConsumerTest {

    @Autowired
    KafkaConsumer kafkaConsumer;

    @Value("${kafka.consumer.group.id}")
    private String topic;

    /**
     * 采用轮询的方式
     */
    @Test
    public void consumerData() {
        System.out.println("进入消费者");
        //指定topic
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        System.out.println("订阅成功");
        try {
            //轮询期间所做的操作应该尽快完成，应为心跳的发送也是在轮询里
            while (true){
                //超时时间，
                ConsumerRecords<String,String> consumerRecords=kafkaConsumer.poll(100);
                System.out.println("消费成功");
                for(ConsumerRecord<String,String> record:consumerRecords){
                    System.out.println("key:"+record.key()+" value:"+record.value());
                }
                //设置enable.auto.commit为false，关闭自动提交  改为手动提交
                //手动同步提交  会影响效率，但是更安全
//                kafkaConsumer.commitSync();  有重试机制
//                kafkaConsumer.commitAsync();   异步提交 不会进行重试，应为可能有更大的偏移量提交成功  例如提交1000 但是别的已经提交成功2000
                //回调
//                kafkaConsumer.commitAsync(new OffsetCommitCallback() {
//                    @Override
//                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
//                        if(e!=null){
//                            for(TopicPartition topicPartition:map.keySet()){
//                                System.out.println("commit failed for offsets {}"+map.get(topicPartition).offset()+e.getMessage());
//                            }
//                        }
//                    }
//                });

                //提交指定位置的偏移量
                //应对环境   一次获取大量数据，防止重读，分批次提交,
//                Map<TcpOperations, OffsetAndMetadata> currentOffsets=new HashMap<>();
//                kafkaConsumer.commitAsync(currentOffsets,null);

//                kafkaConsumer.seek(分片，位置);  调到指定的位置开始读取
//                kafkaConsumer.seekToEnd(topic);   跳到尾部开始读取

            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            kafkaConsumer.close();

        }
    }

}
