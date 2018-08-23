package io.trasnwarp.idc.tar;

import io.trasnwarp.idc.util.FileUtil;
import io.trasnwarp.idc.util.ScanFileUtil;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

public class TarFileStart {
    private static String FTP_DIR;
    private static String SLEEP;
    private static String[] PreList;
    private static String PROV_PREFIX;

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
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void tarFile() throws Exception {
        //扫描FTP下城市文件夹内文件，传给TarProcess处理
        for (String prefix : PreList) {
            List<String> DirList = new ArrayList<String>();
            Pattern p1 = Pattern.compile(prefix);
            DirList = ScanFileUtil.scanFile(FTP_DIR, p1);
            for (String filePath : DirList) {
                File parentfile = new File(filePath);
                //正则：只要符合规则的txt.gz
                Pattern p2 = Pattern.compile("\\d{24}\\.txt\\.gz");
                List<String> subFileList = ScanFileUtil.scanFile(parentfile.getAbsolutePath(), p2);
                ArrayList<String> finishList = new ArrayList<String>();
                for (String finishPath : subFileList) {
                    File finshFile = new File(finishPath);
                    finishList.add(finshFile.getParent() + "/finish_dfr/" + finshFile.getName());
                    File finish = new File(finshFile.getParent() + "/finish_dfr");
                    if (!finish.exists()) {
                        finish.mkdirs();
                    }
                    finshFile.renameTo(new File(finish.getAbsolutePath() + "/" + finshFile.getName()));
                }
                TarProcess tp = new TarProcess(finishList, prefix);
                tp.calculatePart();
            }
        }
    }

    public static void main(String[] args) {
        TarFileStart t = new TarFileStart();
        while (true) {
            try {
                t.tarFile();
                System.out.println("TAR开始下一次扫描......");
                Thread.sleep(Integer.parseInt(SLEEP));
            } catch (Exception e) {
                e.printStackTrace();
                continue;
            }
        }

    }
}
