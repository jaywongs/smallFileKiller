package io.trasnwarp.idc.hdfs;

import io.trasnwarp.idc.util.BakFileUtil;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class AtlerPartition {
    public static Logger log = Logger.getLogger(BakFileUtil.class.getName());
    JDBC jd = new JDBC();

    public void AddID(String tablename, String ID, String HH,String locationPath) throws SQLException {
        Connection conn = jd.connection();
        Statement stmt = conn.createStatement();
        //location \'har:///"+locationPath+"/\'
        String sql = "alter table " + tablename + " add if not exists partition(company_id=" + ID + "," + "hour_id=" + HH + ");";
        log.info(sql);
        stmt.execute(sql);
        stmt.close();
        conn.close();
    }
}