package mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    // 框架在map处理完成之后，将所有key-value对缓存起来，进行分组，然后传递一个组<key,values{}>并调用一次reduce方法
    // values封装为Iterable类型，保存对应key的所有value结果
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        // 业务逻辑：1.遍历value的List进行累加求和； 2.输出对应key的统计结果
        long counter = 0L;
        for(LongWritable value : values) {
            counter += value.get();
        }
        // 将reduce的输出结果写入到context，其后可以保存到相应文件路径下
        context.write(key, new LongWritable(counter));
    }
}
