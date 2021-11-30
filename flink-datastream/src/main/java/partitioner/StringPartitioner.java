package partitioner;

import org.apache.flink.api.common.functions.Partitioner;

public class StringPartitioner implements Partitioner<String> {
    @Override
    public int partition(String key, int numPartitions) {

        System.out.println(numPartitions);

        if ("a.com".equals(key)) {
            return 0;
        } else if ("b.com".equals(key)) {
            return 1;
        } else {
            return 2;
        }
    }
}
