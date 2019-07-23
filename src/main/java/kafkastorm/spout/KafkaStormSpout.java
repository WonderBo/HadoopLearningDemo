package kafkastorm.spout;

import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;

/**
 * @description 将Kafka作为输入数据源封装为Spout
 */
public class KafkaStormSpout {
    // Kafka的主题
    private String kafkaTopic = "demo";
    // 连接ZK的主机和端口
    private String brokerZkStr = "wonder1:2181,wonder2:2181,wonder3:2181";
    // topic在ZK中的根目录（默认情况下，Kafka的topic在ZK上的目录为/brokers，因此此处可以不用设置brokerZkPath参数）
    private String brokerZkPath = "/brokers";
    // 保存消费者偏离值（offset）的ZK目录
    private String zkRoot = "/kafka";
    // Spout的唯一标志
    private String spoutId = "kafkaSpout";

    /**
     *
     * @return
     * @description 从Kafka获取输入流数据
     */
    public KafkaSpout getKafkaSpout() {
        BrokerHosts brokerHosts = new ZkHosts(brokerZkStr, brokerZkPath);
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, kafkaTopic, zkRoot, spoutId);
        spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());

        return new KafkaSpout(spoutConfig);
    }

}
