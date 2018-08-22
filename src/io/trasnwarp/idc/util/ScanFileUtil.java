package io.trasnwarp.idc.util;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class ScanFileUtil {
    public static List<String> scanFile(String parentPath, Pattern p) {
        File parentDir = new File(parentPath);
        File[] subFile = null;
        List<String> fileList = new ArrayList<String>();
        if (parentDir.isDirectory()) {
            subFile = parentDir.listFiles();
        } else {
            System.out.println(parentPath + "is a file");
        }
        if (subFile != null) {
            for (File file : subFile) {
                Matcher m=p.matcher(file.getName());
                if (m.matches()){fileList.add(file.getAbsolutePath());};
            }
        }
        return  fileList;
    }

    public static Map<String, List<String>> fileClassify(ArrayList<String> fileArr) {
        Map<String, List<String>> cityMap = new HashMap<String, List<String>>();
        for (String fileStr : fileArr) {
            File file = new File(fileStr);
            if (file.getName().endsWith(".DS_Store") || file.isDirectory()) {
                continue;
            }
            String[] keyArray = FileUtil.praserFileName(file);
            String key = keyArray[1] + keyArray[0];
            if (cityMap.containsKey(key)) {
                cityMap.get(key).add(file.getAbsolutePath());
            } else {
                List<String> fileList = new ArrayList<String>();
                fileList.add(file.getAbsolutePath());
                cityMap.put(key, fileList);
            }
        }
        return cityMap;
    }

    public static File[] transArr(List<String> tmpList) {
        File[] files = new File[tmpList.size()];
        for (int i = 0; i < tmpList.size(); i++) {
            files[i] = new File(tmpList.get(i));
        }
        return files;
    }

    public static File[] transFileArr(List<File> tmpList) {
        File[] files = new File[tmpList.size()];
        for (int i = 0; i < tmpList.size(); i++) {
            files[i] = tmpList.get(i);
        }
        return files;
    }
}
