package mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @description combiner必须遵循reducer的规范,可以把它看成一种在map任务本地运行的reducer
 *               使用combiner的时候要注意两点：1、combiner的输入输出数据泛型类型要能跟mapper和reducer匹配；2、combiner加入之后不能影响最终的业务逻辑运算结果
 */
public class DataCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {
}
