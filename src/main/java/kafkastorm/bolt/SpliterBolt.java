package kafkastorm.bolt;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @description 同Storm中的Bolt，可以实现 IBasicBolt 接口或者继承 BaseBasicBolt 父类
 */
public class SpliterBolt implements IBasicBolt {
    public void prepare(Map map, TopologyContext topologyContext) {

    }

    /**
     *
     * @param tuple
     * @param basicOutputCollector
     * @description 相关的业务处理逻辑（将语句分割为单词）
     */
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        // 根据Index获取上一个组件传递过来的数据,其中数据封装于Tuple
        String message = tuple.getString(0);

        // 具体处理逻辑
        String[] words = message.split(" ");
        for(String word : words) {
            word = word.trim();
            if(StringUtils.isNotBlank(word)) {
                word = word.toLowerCase();
                basicOutputCollector.emit(new Values(word));
            }
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
        outputFieldsDeclarer.declare(new Fields("word"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
