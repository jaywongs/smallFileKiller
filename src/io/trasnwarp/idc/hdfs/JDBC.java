package io.trasnwarp.idc.hdfs;

import io.trasnwarp.idc.util.FileUtil;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class JDBC {
    private static String INCEPTOR_SERVER_PORT;
    private static String INCEPTOR_USER;
    private static String INCEPTOR_PARAM;
    private static String INCEPTOR_PASS;
    private static String DATABASE;
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    static {
        try {
            Properties prop = FileUtil.getConfig("io.trasnwarp.idc.ftp.properties");
            INCEPTOR_SERVER_PORT = prop.getProperty("INCEPTOR_SERVER_PORT");
            INCEPTOR_USER = prop.getProperty("INCEPTOR_USER");
            INCEPTOR_PARAM = prop.getProperty("INCEPTOR_PARAM");
            DATABASE = prop.getProperty("DATABASE");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public static String getDriverName() {
        return driverName;
    }

    public static void setDriverName(String driverName) {
        JDBC.driverName = driverName;
    }

    public Connection connection() throws SQLException {
        Connection conn;
        try {
            Class.forName(JDBC.getDriverName());
        } catch (Exception e) {
            System.out.println("class load error");
        }
        String jdbcURL = "jdbc:hive2://".concat(INCEPTOR_SERVER_PORT) + "/".concat(DATABASE) + INCEPTOR_PARAM;
        conn = DriverManager.getConnection(jdbcURL, INCEPTOR_USER, INCEPTOR_PASS);
        return conn;
    }
}
