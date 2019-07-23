package kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @description Kafka消费者客户端
 */
public class ConsumerDemo {
    // 消息主题
    private final String topic = "demo";
    // 消费者客户端
    private final Consumer<String, String> consumer;

    public ConsumerDemo() {
        // Kafka参数配置（以ConsumerConfig类或者字符串的形式进行配置）
        Properties properties = new Properties();

        // 消费者连接kafka集群的broker地址
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "wonder1:9092,wonder2:9092,wonder3:9092");
        // 消费者所属消费组的唯一标识
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        // 是否开启自动提交消费位移的功能，默认true
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // 自动提交消费位移的时间间隔，默认5000ms
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        // 如果分区没有初始偏移量或者服务器不存在当前偏移量时，设置使用的偏移量：earliest从头开始消费，latest从最近的开始消费，none抛出异常
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        // Key反序列化方式
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // Value反序列化方式
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        consumer = new KafkaConsumer<String, String>(properties);
    }

    /**
     * @description 接收业务消息
     */
    public void receiveMessage() {
        // 使用Kafka消费者组时，有一个为消费者分配对应分区（partition）的过程，可以使用“自动”subscribe和“手动”assign的方式
        consumer.subscribe(Arrays.asList(topic));

        while (true) {
            // Kafka消费者轮询一次就相当于拉取（poll）一定时间段内broker中可消费的数据，时间一到就立刻返回数据给消费者
            ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
            for(ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.printf("offset = %d, key = %s, value = %s%n",
                        consumerRecord.offset(), consumerRecord.key(), consumerRecord.value());
            }
        }
    }

    public static void main(String[] args) {
        ConsumerDemo consumerDemo = new ConsumerDemo();
        consumerDemo.receiveMessage();
    }
}
