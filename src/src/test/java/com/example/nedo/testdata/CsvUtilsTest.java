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
		assertFalse(Pattern.matches(CsvUtils.HISTORY_REGEXP, "history-.csv"));
		assertFalse(Pattern.matches(CsvUtils.HISTORY_REGEXP, "history-1Acsv"));
		assertTrue(Pattern.matches(CsvUtils.HISTORY_REGEXP, "history-0.csv"));
		assertTrue(Pattern.matches(CsvUtils.HISTORY_REGEXP, "history-9990.csv"));
	}

}
