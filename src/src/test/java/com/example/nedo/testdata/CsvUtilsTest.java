package com.example.nedo.testdata;



import static org.junit.jupiter.api.Assertions.*;

import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;


class CsvUtilsTest {

	/**
	 * HISTORY_REGEXPのテスト
	 */
	@Test
	void testHISTORY_REGEXP() {
		assertFalse(Pattern.matches(CsvUtils.HISTORY_REGEXP, "abc"));
		assertFalse(Pattern.matches(CsvUtils.HISTORY_REGEXP, "history-.csv.gz"));
		assertFalse(Pattern.matches(CsvUtils.HISTORY_REGEXP, "history-1Acsv.gz"));
		assertTrue(Pattern.matches(CsvUtils.HISTORY_REGEXP, "history-0.csv.gz"));
		assertTrue(Pattern.matches(CsvUtils.HISTORY_REGEXP, "history-9990.csv.gz"));
	}

}
