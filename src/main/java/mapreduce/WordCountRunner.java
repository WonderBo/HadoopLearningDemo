package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 用来描述一个特定的作业
 * 比如：该作业分别使用哪个类作为逻辑处理中的map与reduce
 * 也可以指定该作业要处理的数据所在的路径
 * 还可以指定该作业输出的结果放到哪个路径
 * ....
 */
public class WordCountRunner extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new WordCountRunner(), args);
        System.exit(result);
    }

    public int run(String[] strings) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 设置整个job所用的类在哪个jar包（通过传入的class找到job的jar包）
        job.setJarByClass(WordCountRunner.class);

        // 设置该job所使用的mapper和reducer类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        // 设置该job所使用combiner组件类（可选）
        job.setCombinerClass(WordCountReducer.class);
        // 设置自定义的分组逻辑类（可选）
        job.setPartitionerClass(DataPartitioner.class);

        // 设置reduce的任务并发数，应该跟分组的数量保持一致，入参大于分组数则产生空文件，入参（除去1）小于分组数则产生空文件，默认为1
        job.setNumReduceTasks(1);

        // 指定reduce的输出数据key-value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        // 指定map的输出数据key-value的类型（如果与reduce的输出数据类型相同则不用指定）
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 指定要处理的输入数据存放路径
        FileInputFormat.setInputPaths(job, new Path(strings[0]));
        // 指定处理结果的输出数据存放路径
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));

        // 将job提交给集群运行（方法入参表示显示job执行详情）
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
