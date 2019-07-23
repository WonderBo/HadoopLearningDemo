package mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

// 四个泛型参数中，前两个参数是指定mapper输入数据的类型，为提升网络传输效率，Hadoop提供自己的序列化机制，因此建议使用Hadoop的封装类型
// map 和 reduce 的输入输出数据都是以 key-value对的形式封装的
// 默认情况下，框架传递给mapper的输入数据中，key是待处理文本中一行的起始偏移量，value是该行的内容（MapReduce封装屏蔽了数据的输入输出流信息）
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    // MapReduce框架每读一行数据就调用一次map方法
    // 具体业务逻辑就写在map方法体中，而且业务需要处理的数据已经被框架传递进来，保存在方法的参数中 key-value
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 业务逻辑：1.将行内容进行类型转化； 2.按特定分隔符切分； 3.遍历单词数组输出为key-value形式
        String line = value.toString();
        String[] words = StringUtils.split(line, ' ');
        for(String word : words) {
            // 将map的输出结果写入到context，用于reduce的输入数据
            context.write(new Text(word), new LongWritable(1));
        }
    }
}
