package com.xyueji.flink.connectors.mysql;

import com.xyueji.flink.core.model.UserBehavior;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

/**
 * @author xiongzhigang
 * @date 2020-06-12 16:47
 * @description
 */
public class SinkToMysql extends RichSinkFunction<List<UserBehavior>> {
    private PreparedStatement ps;
    PreparedStatement psCheck;
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String sql = "insert into category(sub_category_id, parent_category_id) values(?, ?);";
        ps = connection.prepareStatement(sql);
        String sqlCheck = "select 1 from category where parent_category_id = ?";
        psCheck = connection.prepareStatement(sqlCheck);
    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭连接和释放资源
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    @Override
    public void invoke(List<UserBehavior> value, Context context) throws Exception {

        System.out.println("将要插入：" + value.size() + "条数据");
        for (UserBehavior userBehavior : value) {
            long sub_category_id = userBehavior.getCategoryId() % 6 + 1;
            psCheck.setLong(1, userBehavior.getCategoryId());
            ResultSet resultSet = psCheck.executeQuery();
            if (resultSet.next()) {
                ps.setLong(1, sub_category_id);
                ps.setLong(2, userBehavior.getCategoryId());
                ps.addBatch();
            }
        }

        int[] count = ps.executeBatch();//批量后执行
        System.out.println("成功了插入了" + count.length + "行数据");
        Thread.sleep(2000);
    }

    private Connection getConnection() {
        Connection connection = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://office-computer:3306/flink", "root", "123456");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("-----------mysql get connection has exception , msg = " + e.getMessage());
        }

        return connection;
    }
}
