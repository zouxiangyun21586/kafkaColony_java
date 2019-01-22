package consumer.group;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
 
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 按组消费1
 * @author zxy
 *
 */
public class GroupConsumerTest3 extends Thread {
	private final ConsumerConnector consumer;
    private final String topic;
    private  ExecutorService executor;
	
	public GroupConsumerTest3(String a_zookeeper, String a_groupId, String a_topic){
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig(a_zookeeper, a_groupId));
        this.topic = a_topic;
	}
	
	public void shutdown() {
        if (consumer != null) consumer.shutdown();
        if (executor != null) executor.shutdown();
        try {
            if (!executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS)) {
                System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted during shutdown, exiting uncleanly");
        }
   }
 
    public void run(int a_numThreads) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(a_numThreads));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
 
        // now launch all the threads
        //
        executor = Executors.newFixedThreadPool(a_numThreads);
 
        // now create an object to consume the messages
        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            executor.submit(new ConsumerTest(stream, threadNumber));
            threadNumber++;
        }
    }
	private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "40000");
        props.put("zookeeper.sync.time.ms", "2000");
        props.put("auto.commit.interval.ms", "1000");
        props.put("enable.auto.commit", "false");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("auto.offset.reset", "smallest");
 
        return new ConsumerConfig(props);
    }
	
	public static void main(String[] args) {
		if(args.length < 1){
			System.out.println("Please assign partition number.");
		}
		
        String zooKeeper = "192.168.1.138:2181,192.168.1.175:2181,192.168.1.180:2181";
        String groupId = "jikegrouptest";
        String topic = "tangzihao";
        int threads = Integer.parseInt("1");
 
		GroupConsumerTest3 example = new GroupConsumerTest3(zooKeeper, groupId, topic);
        example.run(threads);
 
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException ie) {
        	ie.printStackTrace();
        }
        example.shutdown();
    }
}
