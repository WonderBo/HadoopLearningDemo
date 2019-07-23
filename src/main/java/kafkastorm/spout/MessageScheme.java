package kafkastorm.spout;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;

/**
 * @description 由于Kafka-Storm主要是对Spout进行封装实现，不能实现declareOutputFields方法，因此需要自定义Scheme类描述Tuple反序列化格式
 */
public class MessageScheme implements Scheme {
    /**
     *
     * @param byteBuffer
     * @return
     * @description 将Kafka输出的ByteBuffer反序列化为String，并封装为Tuple
     */
    public List<Object> deserialize(ByteBuffer byteBuffer) {
        String message = byteBufferToString(byteBuffer);

        return new Values(message);
    }

    /**
     *
     * @return
     * @description 定义Tuple字段，声明Kafka发送的Tuple中的字段名
     */
    public Fields getOutputFields() {
        return new Fields("message");
    }

    /**
     *
     * @param byteBuffer
     * @return
     * @description 将ByteBuffer转化为String（Byte ->（解码器）-> Char -> String）
     */
    public String byteBufferToString(ByteBuffer byteBuffer) {
        CharBuffer charBuffer = null;
        try {
            Charset charset = Charset.forName("UTF-8");
            CharsetDecoder charsetDecoder = charset.newDecoder();
            charBuffer = charsetDecoder.decode(byteBuffer);
            byteBuffer.flip();

            return charBuffer.toString();
        } catch (Exception e) {
            e.printStackTrace();

            return null;
        }
    }

    /**
     *
     * @param string
     * @return
     * @description 将String转化为ByteBuffer
     */
    public ByteBuffer stringToByteBuffer(String string) {
        return ByteBuffer.wrap(string.getBytes());
    }
}
