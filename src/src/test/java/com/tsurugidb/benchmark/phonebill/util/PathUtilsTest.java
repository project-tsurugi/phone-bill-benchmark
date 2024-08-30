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

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.file.Paths;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

class PathUtilsTest {

	@BeforeAll
	static void setUpBeforeClass() throws Exception {
	}

	@BeforeEach
	void setUp() throws Exception {
	}

	@Test
	@SuppressFBWarnings("DMI_HARDCODED_ABSOLUTE_FILENAME")
	final void test() {
		// 文字列で指定
        assertEquals("/mnt/c/foo/bar", PathUtils.toWls("c:\\foo\\bar", true));
        assertEquals("/mnt/e/foo/bar", PathUtils.toWls("e:\\foo\\bar", true));
        assertEquals("/mnt/c/foo/bar", PathUtils.toWls("c:/foo/bar", true));
        assertEquals("c:\\foo\\bar", PathUtils.toWls("c:\\foo\\bar", false));
        assertEquals("e:\\foo\\bar", PathUtils.toWls("e:\\foo\\bar", false));
        assertEquals("c:/foo/bar", PathUtils.toWls("c:/foo/bar", false));

        // Pathで指定するケース
		assertEquals("/mnt/c/foo/bar", PathUtils.toWls(Paths.get("c:\\foo\\bar"), true));
		assertEquals("/mnt/e/foo/bar", PathUtils.toWls(Paths.get("e:\\foo\\bar"), true));
		assertEquals("/mnt/c/foo/bar", PathUtils.toWls(Paths.get("c:/foo/bar"), true));
		assertEquals("c:\\foo\\bar", PathUtils.toWls(Paths.get("c:\\foo\\bar"), false));
		assertEquals("e:\\foo\\bar", PathUtils.toWls(Paths.get("e:\\foo\\bar"), false));
		if (File.separatorChar == '\\') {
			assertEquals("c:\\foo\\bar", PathUtils.toWls(Paths.get("c:/foo/bar"), false));
		} else {
			assertEquals("c:/foo/bar", PathUtils.toWls(Paths.get("c:/foo/bar"), false));
		}

        // フルパスが指定されないケース
        assertEquals("foo/bar", PathUtils.toWls("foo/bar", false));
        Exception e = assertThrows(RuntimeException.class,  () -> PathUtils.toWls("foo/bar", true));
        assertEquals("Not fullpath: foo/bar", e.getMessage());
	}

}
