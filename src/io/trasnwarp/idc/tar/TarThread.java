package io.trasnwarp.idc.tar;

import io.trasnwarp.idc.util.BakFileUtil;

import java.io.File;
import java.io.IOException;

public class TarThread implements Runnable {
    private File[] fileArr;
    private String key;
    private String partID;
    private String harPath;
    private String citykey;

    public TarThread(File[] fileArr, String key, String partID, String harPath, String citykey) {
        this.fileArr = fileArr;
        this.key = key;
        this.partID = partID;
        this.harPath = harPath;
        this.citykey = citykey;
    }

    @Override
    public void run() {
        try {
            TarFileProcess.genPart(fileArr, partID, harPath);
            for (File file : fileArr) {
                BakFileUtil.bakFile(file,citykey);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
