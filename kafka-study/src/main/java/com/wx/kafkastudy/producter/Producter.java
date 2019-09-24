package com.wx.kafkastudy.producter;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

@Component
public class Producter {

    @Value("${kafka.consumer.group.id}")
    private String topic;

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;

    public void productDate(String name){
        ProducerRecord<String,String> record=new ProducerRecord<>(topic,"name",name);
        ListenableFuture<SendResult<String, String>> resultLis= kafkaTemplate.send(record);
        try {
            SendResult<String, String> result1=resultLis.get();
            System.out.println("偏移量:"+result1.getRecordMetadata().offset());
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void productDateUseCallBack(String name){
        ProducerRecord<String,String> record=new ProducerRecord<>(topic,"name",name);

        try {
//            kafkaTemplate.send(record,new DataCallBack());
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}


class DataCallBack implements Callback{

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        System.out.println("偏移量:"+recordMetadata.offset());
    }
}