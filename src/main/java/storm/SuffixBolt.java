package storm;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.io.FileWriter;
import java.util.Map;
import java.util.UUID;

/**
 * @description bolt主要用于流式处理数据
 */
public class SuffixBolt extends BaseBasicBolt {
    // 数据序列化或展示方式
    private FileWriter fileWriter;

    /**
     *
     * @param stormConf
     * @param context
     * @description prepare方法在bolt组件初始化时会被调用一次
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        try {
            fileWriter = new FileWriter("/home/wonder/app/storm/output/" + UUID.randomUUID());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param tuple
     * @param basicOutputCollector
     * @description execute方法用于描述相关的业务处理逻辑，每收到一个Tuple消息就会被调用一次
     */
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        // 根据Field或Index获取上一个组件传递过来的数据,其中数据封装于Tuple
        String upperName = tuple.getStringByField("upperName");

        // 具体处理逻辑（添加为时间后缀）
        String suffixName = upperName + "-" + System.currentTimeMillis();

        // 保存结果数据
        try {
            fileWriter.write(suffixName);
            fileWriter.write("\n");
            fileWriter.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param outputFieldsDeclarer
     * @description 该bolt不需要发送Tuple消息到下一个组件，所以不需要再声明Tuple的字段名
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
