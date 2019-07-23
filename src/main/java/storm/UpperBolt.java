package storm;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * @description bolt主要用于流式处理数据
 */
public class UpperBolt extends BaseBasicBolt {
    /**
     *
     * @param tuple
     * @param basicOutputCollector
     * @description execute方法用于描述相关的业务处理逻辑
     */
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        // 根据Field或Index获取上一个组件传递过来的数据,其中数据封装于Tuple
        String wordName = tuple.getStringByField("originName");

        // 具体处理逻辑（转换为大写格式）
        String upperName = wordName.toUpperCase();

        // 将处理后的结果封装为Tuple并发送给下一个组件
        basicOutputCollector.emit(new Values(upperName));
    }

    /**
     *
     * @param outputFieldsDeclarer
     * @description declareOutputFields方法用于声明该bolt组件发送的Tuple中的字段名
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("upperName"));
    }
}
