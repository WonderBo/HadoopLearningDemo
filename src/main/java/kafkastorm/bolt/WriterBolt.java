package kafkastorm.bolt;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * @description 同Storm中的Bolt，可以实现 IBasicBolt 接口或者继承 BaseBasicBolt 父类
 */
public class WriterBolt implements IBasicBolt {
    // 数据序列化或展示方式
    private FileWriter fileWriter;

    /**
     *
     * @param map
     * @param topologyContext
     * @description Bolt组件初始化时调用一次
     */
    public void prepare(Map map, TopologyContext topologyContext) {
        try {
            // 本地文件
//            fileWriter = new FileWriter("C:\\Users\\13160\\Desktop\\kafkaStorm\\" + UUID.randomUUID());
            // 服务器文件
            fileWriter = new FileWriter("/home/wonder/app/storm/output/" + UUID.randomUUID());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * @param tuple
     * @param basicOutputCollector
     * @description 相关的业务处理逻辑（将文件写入到文件）
     */
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        // 根据Index获取上一个组件传递过来的数据,其中数据封装于Tuple
        String word = tuple.getString(0);

        // 具体处理逻辑
        try {
            fileWriter.write(word);
            fileWriter.write("\n");
            fileWriter.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public void cleanup() {

    }

    /**
     *
     * @param outputFieldsDeclarer
     * @description 定义输出Tuple字段格式
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
