/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
