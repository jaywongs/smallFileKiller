package io.trasnwarp.idc.test;

import io.trasnwarp.idc.hdfs.AtlerPartition;
import io.trasnwarp.idc.util.FileUtil;
import io.trasnwarp.idc.util.VerifyIllegalUtil;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestUtil {
    @Test
    public void testScanFile() {

        //System.out.println( ScanFileUtil.scanFile("/idc",null,null));
    }

    @Test
    public void testPraserFileName() throws IOException, InterruptedException {
        File errorFile = new File("/var/log/apt/history.log");
        System.out.println(FileUtil.praserFileName(errorFile)[1]);
    }

    @Test
    public void testverifyGzip() throws IOException, InterruptedException {
        VerifyIllegalUtil.verifyGzip(new File("/home/thor/下载/ideaIC-2017.3.1-no-jdk.io.trasnwarp.idc.tar.gz"));
    }

    @Test
    public void testRegex() {
//        Pattern p=Pattern.compile("\\d{24}\\.([\\.\\w]*)");
//        Matcher m=p.matcher("201806102149340014711028.txt.gz");
        Pattern p = Pattern.compile("\\d{24}\\.txt\\.gz");
        Matcher m = p.matcher("bj123_fdr");
        System.out.println(m.matches());
    }

//    @io.trasnwarp.idc.test
//    public void testPartFile() throws Exception{
//        TarFileProcess tp = new TarFileProcess(4096);
//        String desPath= "/Users/wangshijie/Downloads/testout/";
//        tp.calculatePart( , desPath);
//    }

    @Test
    public void testPartPartiotion() {
        AtlerPartition atlerPartition = new AtlerPartition();
        String citykey = "bj";
        String key = "201808202211028";

//        try {
//            atlerPartition.AddID(citykey + key.substring(0, 8), key.substring(10, 15), key.substring(8, 10));
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
    }
}
