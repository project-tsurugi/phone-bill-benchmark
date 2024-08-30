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

import org.junit.jupiter.api.Test;

class DateUtilsTest {
	@Test
	void testToDate() {
		assertEquals("2010-01-13", DateUtils.toDate("2010-01-13").toString());
		assertThrows(RuntimeException.class, () -> DateUtils.toDate("Bad String"));
	}

	@Test
	void testToTimestamp() {
		assertEquals("2010-01-13 18:12:21.999", DateUtils.toTimestamp("2010-01-13 18:12:21.999").toString());
		assertEquals("2010-01-13 18:12:21.0", DateUtils.toTimestamp("2010-01-13 18:12:21.000").toString());
		assertThrows(RuntimeException.class, () -> DateUtils.toTimestamp("Bad String"));
	}

	@Test
	void testNextDate() {
		assertEquals(DateUtils.toDate("2020-11-11"), DateUtils.nextDate(DateUtils.toDate("2020-11-10")));
	}


	@Test
	void testNextMonth() {
		assertEquals(DateUtils.toDate("2020-12-01"), DateUtils.nextMonth(DateUtils.toDate("2020-11-10")));
		assertEquals(DateUtils.toDate("2021-01-01"), DateUtils.nextMonth(DateUtils.toDate("2020-12-31")));
		assertEquals(DateUtils.toDate("2021-02-01"), DateUtils.nextMonth(DateUtils.toDate("2021-01-01")));
	}

	@Test
	void testPreviousMonthLastDay() {
		assertEquals(DateUtils.toDate("2020-10-31"), DateUtils.previousMonthLastDay(DateUtils.toDate("2020-11-10")));
		assertEquals(DateUtils.toDate("2020-11-30"), DateUtils.previousMonthLastDay(DateUtils.toDate("2020-12-31")));
		assertEquals(DateUtils.toDate("2020-12-31"), DateUtils.previousMonthLastDay(DateUtils.toDate("2021-01-01")));
	}


}
