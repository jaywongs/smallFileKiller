package io.trasnwarp.idc.tar;

import io.trasnwarp.idc.util.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.*;
import java.net.URI;
import java.net.URLEncoder;
import java.util.*;

public class TarFileProcess {

    private static int BUF_SIZE;
    private static byte[] buf;
    private static Configuration conf;
    private static String HDFS_CONF_PATH;

    static {
        try {
            conf = new Configuration();
            Properties prop = FileUtil.getConfig("ftp.properties");
            HDFS_CONF_PATH = prop.getProperty("HDFS_CONF_PATH");
            BUF_SIZE = Integer.parseInt(prop.getProperty("BUF_SIZE"));
            if (HDFS_CONF_PATH.isEmpty()) {
                System.err.println("Please set property \"HDFS_CONF_PATH\" at least");
                System.exit(1);
            } else {
                Path c = new Path(HDFS_CONF_PATH + "hdfs-site.xml");
                Path h = new Path(HDFS_CONF_PATH + "core-site.xml");
                conf.addResource(c);
                conf.addResource(h);
                conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
                conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
            }
            if (BUF_SIZE <= 0) {
                System.err.println("Please set property \"BUF_SIZE\" at least");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String encodeName(String path) throws UnsupportedEncodingException {
        return URLEncoder.encode(path, "UTF-8");
    }

    private static int getHashCode(String tmp) {
        return tmp.hashCode();
    }

    public static void genPart(File[] fileArr, String partID, String harPathStr) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path harPath = new Path(harPathStr);
        FsPermission permission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
        fs.setPermission(harPath, permission);
        String fullPartName = System.currentTimeMillis() + "-part-" + partID;
        String partPath = harPathStr + "/" + fullPartName;
        FSDataOutputStream partFileStream = fs.create(new Path(URI.create(partPath)));

        //index文件头
        FSDataOutputStream indexFo = fs.create(new Path(URI.create(partPath + "_index")));
        String rootLineStr = encodeName("/") + " dir " + fileArr[0].lastModified() + "+493+hive+hadoop 0 0 ";
        TreeMap<Integer, ArrayList<String>> lineMap = new TreeMap<>();
        long startPos = 0;
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < fileArr.length; i++) {
            File curFile = fileArr[i];
            //index文件信息
            sb.append(String.valueOf(curFile.getName()) + " ");
            String relPath = "/" + curFile.getName();
            int curKey = TarFileProcess.getHashCode(relPath);
            ArrayList curArr = lineMap.get(curKey);
            if (curArr == null) {
                curArr = new ArrayList();
                lineMap.put(curKey, curArr);
            }
            curArr.add(TarFileProcess.encodeName(relPath) + " file " + fullPartName + " " + startPos + " "
                    + curFile.length() + " " + curFile.lastModified() + "+420+hive+hadoop ");
            //part文件拼接
            FileInputStream fi = new FileInputStream(curFile);
            buf = new byte[BUF_SIZE];
            int read = fi.read(buf, 0, BUF_SIZE);
            while (-1 != read) {
                partFileStream.write(buf, 0, read);
                read = fi.read(buf, 0, BUF_SIZE);
            }
            fi.close();
            startPos += curFile.length();
        }
        partFileStream.close();

        //index文件写入
        byte[] rootLine = (rootLineStr + sb.toString() + " \n").getBytes();
        indexFo.write(rootLine);
        for (Map.Entry<Integer, ArrayList<String>> indexLine : lineMap.entrySet()) {
            for (String lineStr : indexLine.getValue()) {
                byte[] line = (lineStr + "\n").getBytes();
                indexFo.write(line);
            }
        }
        indexFo.close();
    }
}

