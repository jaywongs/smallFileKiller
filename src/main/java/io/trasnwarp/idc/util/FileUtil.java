package io.trasnwarp.idc.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

public class FileUtil {
	
	private static String getJarDir() {
		String path = FileUtil.class.getProtectionDomain().getCodeSource().getLocation().getFile();
		try {
			path = java.net.URLDecoder.decode(path, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return new File(path).getParent();
	}
	
	public static Properties getConfig(String filename) throws IOException {
		File file = new File(getJarDir() + "/" + filename);
		InputStream ins = new FileInputStream(file);
		Properties ps = new Properties();
		ps.load(ins);
		return ps;
	}
	public static Properties getConfigByPath(String filename) throws IOException {
//		File file = new File(FileUtil.class.getClassLoader().getResourceAsStream(filename));
		InputStream ins = FileUtil.class.getClassLoader().getResourceAsStream("config/" + filename);
		Properties ps = new Properties();
		ps.load(ins);
		return ps;
	}

	public static InputStream getConfigStream(String filename) throws IOException {
		return FileUtil.class.getClassLoader().getResourceAsStream("config/" + filename);
	}
	
	public static byte[] read(String file) {
		try {
			BufferedInputStream in = new BufferedInputStream(new FileInputStream(file));
			byte buf[] = new byte[in.available()];
			in.read(buf, 0, buf.length);
			in.close();
			return buf;
		} catch (IOException ex) {
			ex.printStackTrace();
			return null;
		}
	}
	
	public static boolean write(String file, byte[] data, boolean append) {
		try {
			BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file, append));
			out.write(data);
			out.close();
			return true;
		} catch (IOException ex) {
			ex.printStackTrace();
			return false;
		}
	}

	public static String[] praserFileName(File file) {
		String filename = file.getName();
		String Hour_id = "";
		String Company_Id = "";
		String[] array = new String[2];
		try {
			Hour_id = filename.substring(0, 10);
			Company_Id = filename.substring(19, 24);
		} catch (Exception e) {
			Hour_id = "error_filename_fdr";
			Company_Id = "error_filename_fdr";
		}
		array[0] = Company_Id;
		array[1] = Hour_id;
		return array;
	}
}
