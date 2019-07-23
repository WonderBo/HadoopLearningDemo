package kafkastorm.topology;

import kafkastorm.bolt.SpliterBolt;
import kafkastorm.bolt.WriterBolt;
import kafkastorm.spout.KafkaStormSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * @description 以Kafka为输入数据源的Topology示例
 */
public class KafkaTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        // 在Topology中添加Spout和Bolt组件
        topologyBuilder.setSpout("kafkaSpout", new KafkaStormSpout().getKafkaSpout());
        topologyBuilder.setBolt("spliterBolt", new SpliterBolt()).shuffleGrouping("kafkaSpout");
        topologyBuilder.setBolt("writerBolt", new WriterBolt(), 4).fieldsGrouping("spliterBolt", new Fields("word"));

        // 配置一些Topology在Storm集群中运行时的参数
        Config config = new Config();
        config.setNumWorkers(4);
        config.setNumAckers(0);
        config.setDebug(false);

        // LocalCluster将Topology提交到本地模拟器运行，便于开发调试
//        LocalCluster localCluster = new LocalCluster();
//        localCluster.submitTopology("kafkaTopology", config, topologyBuilder.createTopology());

        // 将Topology提交到Storm集群中运行
         StormSubmitter.submitTopology("kafkaTopology", config, topologyBuilder.createTopology());
    }
}
