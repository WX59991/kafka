package com.wx.kafkastudy.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Value("${kafka.producer.servers}")
    private String servers;

    @Value("${kafka.producer.retries}")
    private int retries;

    @Value("${kafka.producer.linger}")
    private int linger;

    @Value("${kafka.producer.buffer.memory}")
    private int bugfferMemory;

    public Map<String,Object> produceProperties(){
        Map<String,Object> prop=new HashMap<>();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,servers);
        prop.put(ProducerConfig.RETRIES_CONFIG,retries);
        prop.put(ProducerConfig.LINGER_MS_CONFIG,linger);
        prop.put(ProducerConfig.BUFFER_MEMORY_CONFIG,bugfferMemory);
        //序列化方式
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        return prop;
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(produceProperties());
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

}
