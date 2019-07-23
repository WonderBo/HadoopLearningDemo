package mapreduce;

import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

/**
 * @description Map输出数据分组需要自定义改造两个机制：1、改造分区的逻辑，自定义一个Partitioner；2、自定义reduce task的并发任务数
 */
public class DataPartitioner<KEY, VALUE> extends Partitioner<KEY, VALUE> {
    private static Map<String, Integer> map = new HashMap<String, Integer>();

    static {
        // 一般从数据库等持久层中获取分组映射关系，并保存在内存中方便快速读取（不能将该逻辑放进getPartition方法中，否则每处理一行数据就执行一次）
        map.put("一月", 0);
        map.put("二月", 1);
    }

    @Override
    public int getPartition(KEY key, VALUE value, int i) {
        int patitionerNum = map.get(key.toString());

        return patitionerNum;
    }
}
