package cn.zhubin.sparkstreaming;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;

/**
 *
 * 连接池
 * 复用连接
 *
 */
public class ConnectionPool {

    //队列
    private static LinkedList<Connection> connectionQueue;

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }


    //创建连接池
    public synchronized static Connection getConnection(){
        try {
            if(connectionQueue == null){
                connectionQueue = new LinkedList<Connection>();
                for(int i = 0; i < 5; i++){
                    Connection conn = DriverManager.getConnection("jdbc:mysql://node1:3306/test","root","zhubin");
                    connectionQueue.push(conn);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connectionQueue.poll();
    }

    public static void returnConnection(Connection conn){
        connectionQueue.push(conn);
    }

}
