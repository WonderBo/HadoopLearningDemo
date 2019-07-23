package storm;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

/**
 * @description 组织各个处理组件形成一个完整的处理流程，即topology(类似于mapreduce程序中的job)
 * 并且将该topology提交给storm集群执行，topology提交到集群后将不断运行，除非人为或者异常退出
 */
public class MainTopology{
    public static void main(String[] args) throws Exception {
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        // 在Topology中添加Spout组件
        // parallelism_hint ：4  表示用4个excutor线程来执行该组件
        // setNumTasks(8) 设置该组件执行时的并发task数量，也就意味着1个excutor线程会运行2个task
        topologyBuilder.setSpout("randomSpout", new RandomSpout(), 4).setNumTasks(8);

        // 在Topology中添加大写转换Bolt组件，并且指定它接收randomSpout组件的输出消息
        // 方法【组件1 shuffleGrouping(组件2)】表示:
        //      (1)组件1接收的Tuple消息一定来自于组件2；
        //      (2)组件1和组件2的大量并发task实例之间收发消息时采用的分组策略是随机分组（shuffleGrouping）
        topologyBuilder.setBolt("upperBolt", new UpperBolt(), 4).shuffleGrouping("randomSpout");

        // 在Topology中添加后缀追加Bolt组件，并且指定它接收upperBolt组件的输出消息
        topologyBuilder.setBolt("suffixBolt", new SuffixBolt(), 4).shuffleGrouping("upperBolt");

        // 使用用builder创建一个Topology
        StormTopology demoTopology = topologyBuilder.createTopology();

        // 配置一些Topology在Storm集群中运行时的参数
        Config config = new Config();
        // 设置整个Topology所占用的槽位数，也就是执行该Topology（包括所有组件）的worker进程数量
        config.setNumWorkers(4);
        config.setDebug(true);
        config.setNumAckers(0);

        // 将Topology提交给Storm集群运行
        StormSubmitter.submitTopology("demoTopology", config, demoTopology);
    }
}
