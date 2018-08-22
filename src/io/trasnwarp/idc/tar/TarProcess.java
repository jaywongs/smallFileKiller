package io.trasnwarp.idc.tar;

import io.trasnwarp.idc.hdfs.AtlerPartition;
import io.trasnwarp.idc.util.FileUtil;
import io.trasnwarp.idc.util.ScanFileUtil;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TarProcess {

    private static String PART_SIZE;
    private static String DFS_DIR;
    private ArrayList<String> list;
    private String citykey;
    private static int TAR_THREAD_NUMS;
    private static int partSize;

    static {
        try {
            Properties prop = FileUtil.getConfig("io.trasnwarp.idc.ftp.properties");
            PART_SIZE = prop.getProperty("PART_SIZE");
            DFS_DIR = prop.getProperty("DFS_DIR");
            TAR_THREAD_NUMS = Integer.parseInt(prop.getProperty("TAR_THREAD_NUMS"));
            if (PART_SIZE.equals("")) {
                System.err.println("Please set property \"PART_SIZE\" at least");
                System.exit(1);
            }
            if (TAR_THREAD_NUMS <= 0) {
                System.err.println("Please set property \"TAR_THREAD_NUMS\" at least");
                System.exit(1);
            }
            String split[] = PART_SIZE.split("\\*");
            partSize = Integer.parseInt(split[0]) * Integer.parseInt(split[1]) * Integer.parseInt(split[2])
                    * Integer.parseInt(split[3]);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public TarProcess(ArrayList<String> list, String citykey) {
        this.list = list;
        this.citykey = citykey;
    }

    ExecutorService fixedThreadPool = Executors.newFixedThreadPool(TAR_THREAD_NUMS);

    //根据companyhourid计算part并执行生成part操作
    public void calculatePart() throws Exception {
        String desPath = DFS_DIR + "/" + citykey + "/";
        AtlerPartition atlerPartition = new AtlerPartition();
        Map<String, List<String>> fileMap = ScanFileUtil.fileClassify(list);
        for (String key : fileMap.keySet()) { //key:companyhourid
            String harPath = desPath + key + ".io.trasnwarp.idc.tar";
            atlerPartition.AddID(citykey+key.substring(0,8),key.substring(10,15),key.substring(8,10), harPath);
            List<String> fileList = fileMap.get(key);
            System.out.println(fileList);
            File[] fileArr = ScanFileUtil.transArr(fileList);
            int partID = 0;
            ArrayList fileSubArr = new ArrayList<File>();
            long addedSize = 0;
            for (int i = 0; i < fileArr.length; i++) {
                addedSize += fileArr[i].length();
                if (addedSize <= partSize) {
                    fileSubArr.add(fileArr[i]);
                } else {
                    addedSize = fileArr[i].length();
                    fixedThreadPool.execute(new TarThread(ScanFileUtil.transFileArr(fileSubArr), key, String.valueOf(partID), harPath, citykey));
                    partID++;
                    fileSubArr = new ArrayList<File>();
                    fileSubArr.add(fileArr[i]);
                }
            }
            if (addedSize != 0) {
                fixedThreadPool.execute(new TarThread(ScanFileUtil.transFileArr(fileSubArr), key, String.valueOf(partID), harPath, citykey));
            }
        }
    }
}
