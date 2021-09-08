package com.example.nedo.util;

import java.io.File;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Windowsのファイルパス(フルパス)とWLSのパスに変換するUtil Class
 *
 */
public class PathUtils {
	/**
	 * この値がtrueのときtoWls()は先頭の/mnt/cを除去した文字列を返す
	 */
	private static boolean toLinuxPath = false;

    /**
     * Windowsのファイルパス(フルパス)をWLSのパスに変換する.
     *
     * @param fullPath 変換対象
     * @param isWindows このフラグがtrueのときのみ変換する
     * @return 変換後のパス
     */
    static String toWls(String fullPath, boolean isWindows) {
    	if (isWindows) {
    		Pattern pattern = Pattern.compile("^[A-Za-z]:");
    		Matcher matcher = pattern.matcher(fullPath);
    		if (matcher.find()) {
    			String prefix = "/mnt/" + fullPath.substring(0, 1).toLowerCase();
    			String ret = matcher.replaceFirst(prefix);
    			ret = ret.replaceAll("\\\\", "/");
    			if (toLinuxPath) {
        			ret = ret.replaceAll("/mnt/c/", "/");
    			}
    			return ret;
    		} else {
    			throw new RuntimeException("Not fullpath: " + fullPath);
    		}
    	} else {
    		return fullPath;
    	}
    }

    /**
     * Windowsのファイルパス(フルパス)をWLSのパスに変換する.
     *
     * @param fullPath 変換対象
     * @return 変換後のパス
     */
    public static String toWls(String fullPath) {
    	boolean isWindows = File.separatorChar == '\\';
    	return toWls(fullPath, isWindows);
    }

    /**
     * Windowsのファイルパス(フルパス)をWLSのパスに変換する.
     *
     * @param fullPath 変換対象
     * @return 変換後のパス
     */
    public static String toWls(Path fullPath) {
    	boolean isWindows = File.separatorChar == '\\';
    	return toWls(fullPath.toString(), isWindows);
    }

	public static  void setToLinuxPath() {
		toLinuxPath = true;
	}


}
