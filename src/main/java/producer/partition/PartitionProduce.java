package producer.partition;

import java.util.Map;

import kafka.producer.Partitioner;

/**
 * 往指定的区中插数据
 * @author zxy
 *
 */
public class PartitionProduce implements Partitioner {
    public void configure(Map<String, ?> map) {
    }

	public int partition(Object key, int numPartitions) {
		int partition = 0;
        String stringKey = (String) key;
        int offset = stringKey.lastIndexOf('.');
        if (offset > 0) {
            partition = Integer.parseInt(stringKey.substring(offset + 1)) % numPartitions;
        }
        System.out.println("指定区:" + partition + "||||||");
        return partition;
	}
}

