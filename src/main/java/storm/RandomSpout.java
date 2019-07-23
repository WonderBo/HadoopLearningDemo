package storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * @description spout主要用于获取数据源
 */
public class RandomSpout extends BaseRichSpout {
    private SpoutOutputCollector spoutOutputCollector;
    private String[] words = {"Hadoop","Storm","Apache","Linux","Nginx","Tomcat","Spark"};

    /**
     *
     * @param map
     * @param topologyContext
     * @param spoutOutputCollector
     * @description open方法为数据源的初始化方法，在spout组件实例化时调用一次
     */
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    /**
     * @description nextTuple方法的作用是不断把Tuple发送至下游组件，包括该spout组件的核心逻辑
     */
    public void nextTuple() {
        // 从words数组中随机选择一名称发送出去（实际应用的数据源一般为消息队列等，比如kafka）
        Random random = new Random();
        int idnex = random.nextInt(words.length);
        String originName = words[idnex];

        // 封装为Tuple并发送消息给下一个组件
        spoutOutputCollector.emit(new Values(originName));

        // 每发送一个消息，休眠500ms
        Utils.sleep(500);
    }

    /**
     *
     * @param outputFieldsDeclarer
     * @description declareOutputFields方法用于定义输出字段，声明该spout组件发送的Tuple中的字段名
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("originName"));
    }
}
