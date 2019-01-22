package producer.async;

import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * 异步生产
 * @author zxy
 *
 */
public class ASyncProduce {
	public static void main(String[] args) {
        long events = 10L;
        Random rnd = new Random();
 
        Properties props = new Properties();
        props.put("metadata.broker.list", "192.168.1.138:9092,192.168.1.175:9092,192.168.1.180:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
		//kafka.serializer.DefaultEncoder
        props.put("partitioner.class", "producer.partiton.SimplePartitioner");
		//kafka.producer.DefaultPartitioner: based on the hash of the key
        //props.put("request.required.acks", "1");
		props.put("producer.type", "async");
		//props.put("producer.type", "1");
		// 1: async 2: sync
 
        ProducerConfig config = new ProducerConfig(props);
 
        Producer<String, String> producer = new Producer<String, String>(config);
 
        for (long nEvents = 0; nEvents < events; nEvents++) { 
               long runtime = new Date().getTime();  
               String ip = "192.168.2." + rnd.nextInt(255); 
               String msg = runtime + ",www.example.com," + ip; 
               KeyedMessage<String, String> data = new KeyedMessage<String, String>("jiketest", ip, nEvents+"");
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
