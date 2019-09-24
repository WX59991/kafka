package com.wx.kafkastudy.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.util.HashMap;
import java.util.Map;

@Configuration
@PropertySource(value = "classpath:consumer.properties", ignoreResourceNotFound = true)
public class KafkaConsumerConfig {
    @Value("${kafka.consumer.servers}")
    private String servers;

    @Value("${kafka.consumer.enable.auto.commit}")
    private boolean enableAutoCommit;

    @Value("${kafka.consumer.session.timeout}")
    private String sessionTimeout;

    @Value("${kafka.consumer.auto.commit.interval}")
    private String autoCommitInterval;

    @Value("${kafka.consumer.group.id}")
    private String groupId;

    @Value("${kafka.consumer.auto.offset.reset}")
    private String autoOffsetReset;

    public Map<String, Object> consumerProperties() {
        Map<String, Object> propsMap = new HashMap<>(8);
        propsMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        //是否允许自动提交
        propsMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        //自动提交的时间间隔
        propsMap.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitInterval);
        //会话过期时间
        propsMap.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeout);
        //序列化方式，客户端和服务端必须统一
        propsMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        propsMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        //消费的geoupid
        propsMap.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        propsMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        return propsMap;
    }


    @Bean
    public KafkaConsumer<String,String> createConsumer(){
        Map<String, Object> propsMap=consumerProperties();
        return new KafkaConsumer<String, String>(propsMap);
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerProperties());
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactory(ConsumerFactory<String, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setPollTimeout(1500);
        return factory;
    }
}