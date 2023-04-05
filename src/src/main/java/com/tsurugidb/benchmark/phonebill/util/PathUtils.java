package com.tsurugidb.benchmark.phonebill.util;

import java.nio.file.Path;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Windowsのファイルパス(フルパス)とWLSのパスに変換するUtil Class
 *
 */
public interface PathUtils {
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
    			String prefix = "/mnt/" + fullPath.substring(0, 1).toLowerCase(Locale.ROOT);
    			String ret = matcher.replaceFirst(prefix);
    			ret = ret.replaceAll("\\\\", "/");
    			return ret;
    		} else {
    			throw new RuntimeException("Not fullpath: " + fullPath);
    		}
    	} else {
    		return fullPath;
    	}
    }

    public static String toWls(Path fullPath, boolean isWindows) {
    	return toWls(fullPath.toString(), isWindows);
    }
}
