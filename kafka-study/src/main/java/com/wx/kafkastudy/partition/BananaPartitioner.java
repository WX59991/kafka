package com.wx.kafkastudy.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;

public class BananaPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] bytes1, Cluster cluster) {
        Integer partitionSize=cluster.partitionCountForTopic(topic);
        if(keyBytes==null || !(key instanceof String)){
            throw new InvalidRecordException("参数不合法");
        }

        if("Banana".equals(key.toString())){
            return partitionSize;
        }

        return Math.abs(Utils.murmur2(keyBytes))%(partitionSize-1);
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
