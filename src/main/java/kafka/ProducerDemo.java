package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @description Kafka生产者客户端
 */
public class ProducerDemo {
    // 消息主题
    private final String topic = "demo";
    // 生产者客户端
    private final Producer<String, String> producer;

    public ProducerDemo() {
        // Kafka参数配置（以ProducerConfig类或者字符串的形式进行配置）
        Properties properties = new Properties();

        // 生产者连接kafka集群的broker地址
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "wonder1:9092,wonder2:9092,wonder3:9092");
        // 回令类型: 如果必须等待回令，那么设置acks为all；否则，设置为-1；等待回令会有性能损耗
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        // 消息发送失败重试次数，默认0
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        // 批量提交（ProducerBatch内存区域）大小
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        // 发送延迟等待时间（等待时间内可以追加提交），如果在延迟时间内batch的大小已达到设置值，则消息会被立即发送，不会再等待，默认值0
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        // 生产者客户端中用于缓存消息的缓存区大小，默认32MB
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        // Key序列化方法
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // Value序列化方法
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // 配置分区（Partitioner）选择策略，为可选配置
        // properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "Partitioner类具体路径");

        producer = new KafkaProducer<String, String>(properties);
    }

    /**
     * @description 发送业务消息（读取文件；读取内存数据库；读socket端口）
     * 注意：生产者在发送消息的过程中，会自己默认批量提交。所以，如果单条指令的发送请求，记得发送完后flush才能生效
     */
    public void sendMessage() {
        for(int i = 0; i < 10; i++) {
            // 以Key-Value的格式发送消息（也可以以Value的格式发送消息）
            producer.send(new ProducerRecord<String, String>(topic, "key_" + i, "value_" + i));
        }

        // 关闭Kafka连接
        producer.close();
    }

    public static void main(String[] args) {
        ProducerDemo producerDemo = new ProducerDemo();
        producerDemo.sendMessage();
    }
}
