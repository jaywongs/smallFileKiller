package io.trasnwarp.idc.test;

import io.trasnwarp.idc.hdfs.JDBC;
import io.trasnwarp.idc.util.BakFileUtil;
import io.trasnwarp.idc.util.FileUtil;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

public class CreateTableStart {
    private static String[] PROV_LIST;
    private static String COL_NAME_TYPE;
    private static String PARTITION_KEY;
    private static String PROV_PREFIX;
    private static String[] P_KEYS;
    private static int DAYS;
    public static Logger log = Logger.getLogger(BakFileUtil.class.getName());

    static {
        try {
            Properties prop = FileUtil.getConfig("ftp.properties");
            PROV_PREFIX = prop.getProperty("PROV_PREFIX");
            PARTITION_KEY = prop.getProperty("PARTITION_KEY");
            COL_NAME_TYPE = prop.getProperty("COL_NAME_TYPE");
            PROV_LIST = PROV_PREFIX.split(",");
            P_KEYS = PARTITION_KEY.split(",");
            DAYS = Integer.parseInt(prop.getProperty("DAYS"));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    JDBC jd = new JDBC();

    public void AutoTable() throws SQLException {
        Connection conn = jd.connection();
        Statement stmt = conn.createStatement();
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.DAY_OF_YEAR, DAYS);
        Date htime = calendar.getTime();
        String time = df.format(htime);
        for (String pv : PROV_LIST) {
            String tablename = pv + time;
            String sql1 = "drop table " + tablename;
            String sql = "create  table IF NOT EXISTS " + tablename + "(" + COL_NAME_TYPE + ")" + " PARTITIONED BY(" + P_KEYS[0] + " string,"
                    + " " + P_KEYS[1] + " string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;";
            log.info(sql);
            stmt.execute(sql1);
            stmt.execute(sql);
        }
        stmt.close();
        conn.close();
    }

    public static void main(String[] args) throws SQLException {
        CreateTableStart create = new CreateTableStart();
        create.AutoTable();
    }
}
