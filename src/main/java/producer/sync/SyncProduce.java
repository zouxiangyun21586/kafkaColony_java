package producer.sync;
import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * 同步生产
 * @author zxy
 *
 */
public class SyncProduce {
	public static void main(String[] args) {
        long events = 10L;
        Random rnd = new Random();
 
        Properties props = new Properties();
        props.put("metadata.broker.list", "192.168.1.138:9092,192.168.1.175:9092,192.168.1.180:9092"); // 主机地址+端口号
        props.put("serializer.class", "kafka.serializer.StringEncoder"); // 消息序列化类
		//kafka.serializer.DefaultEncoder // (默认)字节序列化
        props.put("partitioner.class", "producer.partiton.SimplePartitioner"); // 自定义的分区算法
		//kafka.producer.DefaultPartitioner: based on the hash of the key // 默认的分区算法
        props.put("request.required.acks", "1");
//        props.put("auto.offset.reset", "smallest");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		//0;  绝不等确认  1:   leader的一个副本收到这条消息，并发回确认 -1：   leader的所有副本都收到这条消息，并发回确认
 
        ProducerConfig config = new ProducerConfig(props);

        Producer<String, String> producer = new Producer<String, String>(config);
 
        for (long nEvents = 0; nEvents < events; nEvents++) { 
               long runtime = new Date().getTime();  
               String ip = "192.168.2." + rnd.nextInt(255); // 随机数  并不是将东西发送到某个ip里
               String msg = runtime + ",www.example.com," + ip; 
			   //eventKey必须有（即使自己的分区算法不会用到这个key，也不能设为null或者""）,否者自己的分区算法根本得不到调用
               KeyedMessage<String, String> data = new KeyedMessage<String, String>("tangzihao", ip, nEvents+"");
			   												//			 eventTopic, eventKey, eventBody
               producer.send(data);
			   try {
                   Thread.sleep(1000);
               } catch (InterruptedException ie) {
            	   ie.printStackTrace();
               }
        }
        producer.close();
    }
	
}
