package mapreduce;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @description 用于封装需要传输和处理的数据
 * DataBean 是我们自定义的一种数据类型 ,要在hadoop的各个节点之间传输，因此应该遵循Hadoop的序列化机制,则必须实现Hadoop相应的序列化接口
 */
// WritableComparable<T>接口包含Writable, Comparable<T>两个接口
// Writable为Hadoop的序列化接口，由于目标为网络传输与批量计算，因此Hadoop的序列化机制不会传递对象的继承结构信息（而JDK自带序列化机制会包含）
// Comparable<T>为JDK中的对象比较接口，实现该接口并作为Map的输出Key可以实现Hadoop的自定义排序
public class DataBean implements WritableComparable<DataBean> {
    private String text;
    private long num1;
    private long num2;
    private long sum;

    // 在反序列化时，反射机制需要调用空参构造函数，因此必须显示定义一个空参构造函数
    public DataBean() {

    }

    // 为方便初始化对象，加入一个带参的构造函数
    public DataBean(String text, long num1, long num2, long sum) {
        this.text = text;
        this.num1 = num1;
        this.num2 = num2;
        this.sum = num1 + num2;
    }

    public int compareTo(DataBean o) {
        return sum > o.getSum() ? -1 : 1;
    }

    // 将对象的数据序列化到流中
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(text);
        dataOutput.writeLong(num1);
        dataOutput.writeLong(num2);
        dataOutput.writeLong(sum);
    }

    // 从数据流中反序列出对象的数据
    // 从数据流中读出对象字段时，必须跟序列化时的顺序保持一致
    public void readFields(DataInput dataInput) throws IOException {
        this.text = dataInput.readUTF();
        this.num1 = dataInput.readLong();
        this.num2 = dataInput.readLong();
        this.sum = dataInput.readLong();
    }

    // 最后计算结果写入文件时便于数据展示
    @Override
    public String toString() {
        return  "text='" + text + '\'' +
                ", num1=" + num1 +
                ", num2=" + num2 +
                ", sum=" + sum +
                '}';
    }

    public String getText() {
        return text;
    }
    public void setText(String text) {
        this.text = text;
    }
    public long getNum1() {
        return num1;
    }
    public void setNum1(long num1) {
        this.num1 = num1;
    }
    public long getNum2() {
        return num2;
    }
    public void setNum2(long num2) {
        this.num2 = num2;
    }
    public long getSum() {
        return sum;
    }
    public void setSum(long sum) {
        this.sum = sum;
    }
}
