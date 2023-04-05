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
