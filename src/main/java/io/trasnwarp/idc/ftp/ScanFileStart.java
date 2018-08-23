package io.trasnwarp.idc.ftp;

import io.trasnwarp.idc.util.FileUtil;
import io.trasnwarp.idc.util.ScanFileUtil;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

public class ScanFileStart {
    private static String FTP_DIR;
    private static String SLEEP;
    private static String[] PreList;
    private static String PROV_PREFIX;
    private static String SCAN_THREAD_NUMS;

    static {
        try {
            Properties prop = FileUtil.getConfigByPath("ftp.properties");
            FTP_DIR = prop.getProperty("FTP_DIR");
            if (FTP_DIR.isEmpty()) {
                System.err.println("Please set property \"FTP_DIR\" at least");
                System.exit(1);
            }
            SLEEP = prop.getProperty("SLEEP");
            if (SLEEP.isEmpty()) {
                System.err.println("Please set  SLEEP  at least");
                System.exit(1);
            }
            PROV_PREFIX = prop.getProperty("PROV_PREFIX");
            PreList = PROV_PREFIX.split(",");
            int PROV_NUM = PreList.length;
            if (PROV_PREFIX.isEmpty() && PROV_NUM == 31) {
                System.err.println("Please set property 31 \"PROV_PREFIX\"");
                System.exit(1);
            }
            SCAN_THREAD_NUMS = prop.getProperty("SCAN_THREAD_NUMS");
            if (SCAN_THREAD_NUMS.isEmpty()) {
                System.err.println("Please set  \"SCAN_THREAD_NUMS\" at least");
                System.exit(1);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static ExecutorService fixedThreadPool = Executors.newFixedThreadPool(Integer.valueOf(SCAN_THREAD_NUMS));

    //扫描FTP_DIR
    public static void scanFile() {
        for (String prefix : PreList) {
            List<String> DirList = new ArrayList<String>();
            //正则只扫描类似bj123_fdr格式的文件夹
            String regex = prefix + "\\d{3}" + "_fdr";
            Pattern p = Pattern.compile(regex);
            DirList = ScanFileUtil.scanFile(FTP_DIR, p);
            for (String filePath : DirList) {
                File parentfile = new File(filePath);
                File[] tempList = parentfile.listFiles();
                for (File file : tempList) {
                    String destPath = FTP_DIR + "/" + prefix;
                    //多线程验证压缩文件是否合法，并移动至所属城市文件夹下
                    fixedThreadPool.execute(new VerifyThread(file.getAbsolutePath(), destPath, prefix));
                }
            }
        }
    }

    public static void main(String[] args) {
        ScanFileStart s = new ScanFileStart();
        while (true) {
            try {
                s.scanFile();
                System.out.println("SCAN开始下一次扫描......");
                Thread.sleep(Integer.parseInt(SLEEP));
            } catch (InterruptedException e) {
                e.printStackTrace();
                continue;
            }
        }
    }
}
