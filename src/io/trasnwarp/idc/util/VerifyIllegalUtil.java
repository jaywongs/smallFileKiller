package io.trasnwarp.idc.util;

import java.io.File;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class VerifyIllegalUtil {
    public static boolean verifyGzip(File file) throws InterruptedException, IOException {
        Process process;
        process = Runtime.getRuntime().exec("gzip -t " + file);
        int code = process.waitFor();
        if (code != 0) {
            return false;
        } else {
            return true;
        }
    }

    public static String check(File file) throws IOException, InterruptedException {
        String filename = file.getName();
        Pattern p=Pattern.compile("\\d{24}\\.([\\.\\w]*)");
        Matcher m=p.matcher(filename);
        if (!m.matches()) {
            return "FILENAME_ERROR";
        } else if (filename.endsWith(".txt.gz") && !(filename.endsWith(".tmp")) && verifyGzip(file)) {
            return "LEGAL";
        } else if (!(filename.endsWith(".txt.gz")) && !(filename.endsWith(".tmp"))) {
            return "ILLEGAL";
        } else if (!(verifyGzip(file)) && !(filename.endsWith(".tmp"))) {
            return "UNZip";
        } else if (!filename.endsWith(".txt.gz") && filename.endsWith(".tmp")) {
            return "TMP";
        } else {
            return "UnKnown";
        }

    }
}
