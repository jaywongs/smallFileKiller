package io.trasnwarp.idc.tar;

import io.trasnwarp.idc.util.BakFileUtil;

import java.io.File;
import java.io.IOException;

public class TarThread implements Runnable {
    private File[] fileArr;
    private String key;
    private String partID;
    private String tarPath;
    private String citykey;

    TarThread(File[] fileArr, String key, String partID, String tarPath, String citykey) {
        this.fileArr = fileArr;
        this.key = key;
        this.partID = partID;
        this.tarPath = tarPath;
        this.citykey = citykey;
    }

    @Override
    public void run() {
        try {
            TarFileProcess.genPart(fileArr, partID, tarPath);
            for (File file : fileArr) {
                BakFileUtil.bakFile(file,citykey);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
