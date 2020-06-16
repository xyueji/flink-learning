package com.xyueji.flink.connectors.mysql;

import com.xyueji.flink.core.model.Student;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

/**
 * @author xiongzhigang
 * @date 2020-06-03 19:58
 * @description
 */
public class SourceFromMysql extends RichSourceFunction<Student> {
    private static Connection connection;
    private PreparedStatement ps;
    private final static Logger log = LoggerFactory.getLogger(SourceFromMysql.class);

    static {
        connection = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://office-computer:3306/flink", "root", "123456");
        } catch (Exception e) {
            e.printStackTrace();
            log.error("-----------mysql get connection has exception , msg = ", e);
            System.out.println("-----------mysql get connection has exception , msg = " + e.getMessage());
        }
    }

    public SourceFromMysql() {

    }

    public static Connection getConnection() {
        return connection;
    }

    public static void setConnection(Connection connection) {
        SourceFromMysql.connection = connection;
    }

    public PreparedStatement getPs() {
        return ps;
    }

    public void setPs(PreparedStatement ps) {
        this.ps = ps;
    }

    public static Logger getLog() {
        return log;
    }

    @Override
    public void run(SourceContext<Student> sourceContext) throws Exception {
        String sql = "select * from student;";
        ps = connection.prepareStatement(sql);

        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            Student student = new Student(
                    resultSet.getInt("id"),
                    resultSet.getString("name").trim(),
                    resultSet.getString("password").trim(),
                    resultSet.getInt("age"));
            sourceContext.collect(student);
        }
    }

    @Override
    public void cancel() {
        if (connection != null) { //关闭连接和释放资源
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
