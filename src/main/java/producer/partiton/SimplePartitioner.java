package producer.partiton;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
 
/**
 * 指定区生产
 * 自定义的分区算法
 * @author zxy
 *
 */
public class SimplePartitioner implements Partitioner {
    public SimplePartitioner (VerifiableProperties props) {
 
    }
 
    // 传过来的随机参数   分区数
    public int partition(Object key, int a_numPartitions) {
        int partition = 0;
        String stringKey = (String) key;
        int offset = stringKey.lastIndexOf('.');
        if (offset > 0) {
           partition = Integer.parseInt( stringKey.substring(offset+1)) % a_numPartitions; // 取模
        }
        System.out.println("分区号：" + partition + "  随机数：" + key);
        return partition; // 分区号  (是一个整数, 0 ~ 分区数-1之间的数)
  }
 
}
