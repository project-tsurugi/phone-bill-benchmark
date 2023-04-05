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
