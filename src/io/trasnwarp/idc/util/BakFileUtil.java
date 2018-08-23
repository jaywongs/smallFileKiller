package io.trasnwarp.idc.util;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public abstract class BakFileUtil {
    private static String FTP_DIR;
    public static Logger log = Logger.getLogger(BakFileUtil.class.getName());
    public static DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    static {
        try {
            Properties prop = FileUtil.getConfig("ftp.properties");
            FTP_DIR = prop.getProperty("FTP_DIR");
            if (FTP_DIR.isEmpty()) {
                System.err.println("Please set property \"FTP_DIR\" at least");
                System.exit(1);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void bakFile(File file, String city)  {
        String[] key = FileUtil.praserFileName(file);
        File dir = new File(FTP_DIR + "/" + city);
        if (!(dir.exists())) {
            dir.mkdir();
        }
        if (dir.exists() && dir.isDirectory()) {
            File bakdir = new File(FTP_DIR + "/" + city + "/bak_fdr");
            File errordir = new File(FTP_DIR + "/" + city + "/bak_error_fdr");
            if (!(bakdir.exists())) {
                bakdir.mkdirs();
            }
            if (!(errordir.exists())) {
                errordir.mkdirs();
            }
            File bak_dir = new File(bakdir.getAbsolutePath() + "/" + key[1]);
            if (!(bak_dir.exists())) {
                bak_dir.mkdirs();
            }
            String filename = file.getName();
            Long st = System.currentTimeMillis();
            String StartTime = df.format(new Date(st));
            if (!file.renameTo(new File(bak_dir.getAbsolutePath() + "/" + filename))) {
                long et = System.currentTimeMillis();
                String EndTime = df.format(new Date(et));
                log.info(key[0] + "," + filename + "," + (et - st) + "ms" + "," + StartTime + "," + EndTime + "," + key[1] + ",S,bak failed");
            } else {
                long et = System.currentTimeMillis();
                String EndTime = df.format(new Date(et));
                log.info(key[0] + "," + filename + "," + (et - st) + "ms" + "," + StartTime + "," + EndTime + "," + key[1] + ",S,bak successed");
            }
        }
    }

    public static void bakErrorFile(File file, String city, String[] key) {
        File dir = new File(FTP_DIR + "/" + city);
        if (!(dir.exists())) {
            dir.mkdir();
        }
        if (dir.exists() && dir.isDirectory()) {
            File errordir = new File(FTP_DIR + "/" + city + "/bak_error_fdr");
            if (!(errordir.exists())) {
                errordir.mkdirs();
            }
            File error_dir = new File(errordir.getAbsolutePath() + "/" + key[1]);
            if (!(error_dir.exists())) {
                error_dir.mkdirs();
            }
            String filename = file.getName();
            Long st = System.currentTimeMillis();
            String StartTime = df.format(new Date(st));
            if (!file.renameTo(new File(error_dir.getAbsolutePath() + "/" + filename))) {
                long et = System.currentTimeMillis();
                String EndTime = df.format(new Date(et));
                log.info(key[0] + "," + filename + "," + (et - st) + "ms" + "," + StartTime + "," + EndTime + "," + key[1] + ",S,bak_illegal_or_Unzip failed");
            } else {
                long et = System.currentTimeMillis();
                String EndTime = df.format(new Date(et));
                log.info(key[0] + "," + filename + "," + (et - st) + "ms" + "," + StartTime + "," + EndTime + "," + key[1] + ",S,bak_illegal_or_Unzip successed ");
            }
        }
    }
}
