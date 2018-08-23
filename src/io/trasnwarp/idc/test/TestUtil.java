package io.trasnwarp.idc.test;

import io.trasnwarp.idc.hdfs.AtlerPartition;
import io.trasnwarp.idc.tar.TarFileSystem;
import io.trasnwarp.idc.util.FileUtil;
import io.trasnwarp.idc.util.VerifyIllegalUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestUtil {
    private static Configuration conf;
    private static String HDFS_CONF_PATH = "./";

    static {
        conf = new Configuration();
        Path c = new Path(HDFS_CONF_PATH + "hdfs-site.xml");
        Path h = new Path(HDFS_CONF_PATH + "core-site.xml");
        conf.addResource(c);
        conf.addResource(h);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
    }
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

    @Test
    public void testTarFile() throws Exception {
        URI uri = URI.create("tar:///user/wsj/data/bj/201806102011028.tar");
        TarFileSystem tf = new TarFileSystem();
        tf.initialize(uri, conf);
        Path tarPath = new Path("tar:///user/wsj/data/bj/201806102011028.tar");
        FileStatus fileStatus = tf.getFileStatus(tarPath);
        System.out.println(fileStatus);

        RemoteIterator it =  tf.listFiles(tarPath, true);
        while (it.hasNext()){
            System.out.println(it.next());
        }
    }

    @Test
    public void testTarFileReadSingle() throws Exception {
        URI uri = URI.create("tar:///user/wsj/data/bj/201806102011028.tar/201806102049341010711028.txt.gz");
        TarFileSystem tf = new TarFileSystem();
        tf.initialize(uri, conf);
        Path tarPath = new Path("tar:///user/wsj/data/bj/201806102011028.tar/201806102049341010711028.txt.gz");

        FSDataInputStream in =  tf.open(tarPath);
        byte[] buf = new byte[4096];
        int read = 0;
        while (read != -1){
            read = in.read(buf, 0, buf.length);
        }
    }
}
