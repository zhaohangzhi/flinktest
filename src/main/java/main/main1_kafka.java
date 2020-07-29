package main;

import com.alibaba.fastjson.JSON;
import main.po.Student;
import main.sink.sink_postgres;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

public class main1_kafka {
    private static  final Logger LOGGER = LoggerFactory.getLogger(main1_kafka.class);
    public static void main(String[] args) {
        try {
            final  StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            Properties props = new Properties();
            props.put("bootstrap.servers", "10.120.22.23:9092");
            props.put("zookeeper.connect", "10.120.22.23:2181");
            props.put("group.id", "metric-group");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("auto.offset.reset", "latest");
            SingleOutputStreamOperator<Student> student = env.addSource(new FlinkKafkaConsumer011<>(
                    "zhz_test",   //这个 kafka topic 需要和上面的工具类的 topic 一致
                    new SimpleStringSchema(),
                    props)).setParallelism(1)
                    .map(string -> {
                                LOGGER.info("string="+string);
                               return JSON.parseObject(string, Student.class);
                            }
                            ); //Fastjson 解析字符串成 student 对象
            LOGGER.info("main");
            student.addSink(new sink_postgres());
            env.execute("zhz");

        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
