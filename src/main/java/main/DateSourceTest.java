package main;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.log4j.Logger;
import java.util.ArrayList;

public class DateSourceTest {
    private static  final Logger LOGGER = Logger.getLogger(DateSourceTest.class);
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ArrayList<Integer> strings = new ArrayList<>();
        strings.add(1);
        strings.add(1);
        strings.add(1);
        strings.add(1);
        strings.add(1);
        strings.add(2);
        DataStreamSource<Integer> integerDataStreamSource = env.fromCollection(strings);
        // 记录debug级别的信息
        LOGGER.debug("This is debug message.");
        // 记录info级别的信息
        LOGGER.info("This is info message.");
        // 记录warn级别的信息
        LOGGER.info("This is warn message.");
        // 记录error级别的信息
        LOGGER.error("This is error message.");
    }
}
