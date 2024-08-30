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
