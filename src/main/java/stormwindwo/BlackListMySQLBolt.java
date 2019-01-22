package stormwindwo;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

//将上一级的数据，存储到Mysql数据库中
public class BlackListMySQLBolt extends BaseRichBolt {

    private static final String driver = "com.mysql.jdbc.Driver";
    private static final String url = "jdbc:mysql://162.168.18.21:3306/demo";
    private static final String user = "demo";
    private static final String password = "Welcome_1";
    private String sql;

    //注册驱动
    static {
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        /**
         * 每10秒钟，统计过去30秒内访问频率超过4次的用户信息 ----> 写到MySQL中
         */
        //得到数据
        int userid = tuple.getIntegerByField("userid");
        int pv = tuple.getIntegerByField("PV");

        /**
         * 如果MySQL不存在该userid，就执行插入；如果已经存在就执行更新操作;
         */
        sql = "insert into myresult(userid,PV) values(" + userid + "," + pv + ") on duplicate key update PV=PV+" + pv;

        if (conn != null && statement != null) {
            try {
                statement.execute(sql);
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if (statement != null) {
                    try {
                        statement.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }

                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    Connection conn;
    Statement statement;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        try {
            conn = DriverManager.getConnection(url, user, password);
            statement = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
