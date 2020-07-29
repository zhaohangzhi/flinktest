package main.sink;

import main.po.Student;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * 自定义的sink
 */
public class sink_postgres extends RichSinkFunction<Student> {
    private static final Logger LOGGER = LoggerFactory.getLogger(sink_postgres.class);
    PreparedStatement ps;
    private Connection connection;

    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
     * open方法在sink第一次启动时调用，一般用于sink的初始化操作
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters)  {
        try {
            super.open(parameters);
            String url_postgre = "jdbc:postgresql://10.120.22.246:5432/ai" ;
            connection = DriverManager.getConnection(url_postgre, "postgres", "postgres");
            String sql = "insert into c_sink(c1, c2, c3, c4) values(?, ?, ?, ?);";
            ps = this.connection.prepareStatement(sql);
            LOGGER.info("Opened CalendarHBaseSink");
        }catch (Exception e){
            e.printStackTrace();
            LOGGER.error("Opened  Exception",e);
        }

    }


    @Override
    public void close() throws Exception {
        try {
            super.close();
            //关闭连接和释放资源
            if (connection != null) {
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
            LOGGER.info("Closed CalendarHBaseSink");
        }catch (Exception e){
            e.printStackTrace();
            LOGGER.error("Closed Exception",e);
        }

    }
    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     * invoke方法是sink数据处理逻辑的方法，source端传来的数据都在invoke方法中进行处理
     * 	其中invoke方法中第一个参数类型与RichSinkFunction<String>中的泛型对应。第二个参数
     * 	为一些上下文信息
     *
     * @param value
     * @param context
     * @throws Exception
     */
    public void invoke(Student value, Context context) throws Exception {
        try {
            //组装数据，执行插入操作
            ps.setString(1, value.getId()+"");
            ps.setString(2, value.getName());
            ps.setString(3, value.getPassword());
            ps.setString(4, value.getAge()+"");
            ps.executeUpdate();
            LOGGER.info("executeUpdate invoke");
        }catch (Exception e){
            e.printStackTrace();
            LOGGER.error(" invoke Exception",e);
        }
    }


}
