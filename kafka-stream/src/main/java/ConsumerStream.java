import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ConsumerStream {

    ConsumerConnector consumerConnector;

    private String groupId="topic_tt_test";

    private static String ZK_CONNECT="127.0.0.1:2181";

    public ConsumerConnector createConsumerConnector(){
        Properties prop=new Properties();
        prop.put("group.id", groupId+"2");
        prop.put("zookeeper.connect", ZK_CONNECT);
        prop.put("zookeeper.session.timeout.ms", "3000000");
        prop.put("zookeeper.sync.time.ms", "2000");
        ConsumerConfig config = new ConsumerConfig(prop);
        return Consumer.createJavaConsumerConnector(config);
    }

    public void getData(){
        consumerConnector=createConsumerConnector();
        Map<String, Integer> topicCountMap = new HashMap<>();
        // 设置每个topic开几个线程   线程数可与分片数对应，一个分片一个消费者
        topicCountMap.put(groupId, 1);

        // 获取stream
        Map<String, List<KafkaStream<byte[], byte[]>>> streams = consumerConnector.createMessageStreams(topicCountMap);

        //如果指定一个topic多个线程，就需要用多线程处理返回的信息
        for (KafkaStream<byte[], byte[]> stream : streams.get(groupId)) {
            ConsumerIterator<byte[], byte[]> streamIterator=stream.iterator();
            //会一直阻塞等待到有数据  ,知道程序停下才会结束循环
            while(streamIterator.hasNext()){
                MessageAndMetadata<byte[], byte[]> message =streamIterator.next();
                System.out.println(new String(message.message()));
            }
        }
    }

    public static void main(String[] args) {
        new ConsumerStream().getData();
    }
}
