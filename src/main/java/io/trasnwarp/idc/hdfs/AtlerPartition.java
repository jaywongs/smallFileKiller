package io.trasnwarp.idc.hdfs;

import io.trasnwarp.idc.util.BakFileUtil;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class AtlerPartition {
    private static Logger log = Logger.getLogger(BakFileUtil.class.getName());
    private JDBC jd = new JDBC();

    public void AddID(String tablename, String ID, String HH,String locationPath) {
        Connection conn = null;
        try {
            conn = jd.connection();
            Statement stmt = conn.createStatement();
            //测试完成后location位置为tar文件内
//            String sql = "alter table " + tablename + " add if not exists partition(company_id=" + ID + "," +
//                    "hour_id=" + HH + ")location \'tar:///"+locationPath+"/\';";
            String sql = "alter table " + tablename + " add if not exists partition(company_id=" + ID + "," +
                    "hour_id=" + HH + ");";
            log.info(sql);
            stmt.execute(sql);
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
