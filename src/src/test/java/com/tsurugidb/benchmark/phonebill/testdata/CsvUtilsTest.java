package com.tsurugidb.benchmark.phonebill.testdata;



import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Paths;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;


class CsvUtilsTest {

	/**
	 * HISTORY_REGEXPのテスト
	 */
	@Test
	void testHISTORY_REGEXP() {
		assertFalse(Pattern.matches(CsvUtils.HISTORY_REGEXP, "abc"));
		assertFalse(Pattern.matches(CsvUtils.HISTORY_REGEXP, "history-.csv"));
		assertFalse(Pattern.matches(CsvUtils.HISTORY_REGEXP, "history-1Acsv"));
		assertTrue(Pattern.matches(CsvUtils.HISTORY_REGEXP, "history-0.csv"));
		assertTrue(Pattern.matches(CsvUtils.HISTORY_REGEXP, "history-9990.csv"));
	}

	/**
	 * isCSV()のテスト
	 */
	@Test
	@SuppressFBWarnings("DMI_HARDCODED_ABSOLUTE_FILENAME")
	void testIsCsv() {
		assertTrue(CsvUtils.isCsv(Paths.get("history-0.csv")));
		assertFalse(CsvUtils.isCsv(Paths.get("foo.csv")));
		assertFalse(CsvUtils.isCsv(Paths.get("/")));
		assertFalse(CsvUtils.isCsv(null));
	}

}
