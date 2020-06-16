package com.xyueji.flink.connectors.mysql;

import com.xyueji.flink.core.model.Student;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author xiongzhigang
 * @date 2020-06-03 20:13
 * @description
 */
public class AddMysqlDataSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new SourceFromMysql()).keyBy(new KeySelector<Student, Object>() {
            @Override
            public Object getKey(Student student) throws Exception {
                return student.getAge();
            }
        }).print();

        env.execute("Flink add mysql datasource");
    }
}
