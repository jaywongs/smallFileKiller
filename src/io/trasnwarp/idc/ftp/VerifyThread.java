package io.trasnwarp.idc.ftp;

import io.trasnwarp.idc.util.BakFileUtil;
import io.trasnwarp.idc.util.FileUtil;
import io.trasnwarp.idc.util.VerifyIllegalUtil;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;

public class VerifyThread implements Runnable {
    private String srcPath;
    private String destPath;
    private String city;
    public static Logger log = Logger.getLogger(BakFileUtil.class.getName());

    public VerifyThread(String srcPath, String destPath, String city) {
        this.srcPath = srcPath;
        this.destPath = destPath;
        this.city = city;
    }

    @Override
    public void run() {
        try {
            File filesrc = new File(srcPath);
            String result = VerifyIllegalUtil.check(filesrc);
            if (result.equals("LEGAL")) {
                File filedest = new File(destPath);
                if (!filedest.exists()) {
                    filedest.mkdirs();
                }
                filesrc.renameTo(new File(destPath + "/" + filesrc.getName()));
                log.info(filesrc.getName() + " is LEGAL");

            } else if (result.equals("TMP")) {/*Nothing 文件未完成留给下一次扫描*/} else {
                BakFileUtil.bakErrorFile(filesrc, city, FileUtil.praserFileName(filesrc));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
