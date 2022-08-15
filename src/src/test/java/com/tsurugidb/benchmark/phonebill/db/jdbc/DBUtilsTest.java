package com.tsurugidb.benchmark.phonebill.db.jdbc;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.ParseException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.app.Config;

class DBUtilsTest {
	/**
	 * getConnection()のテスト
	 * @throws SQLException
	 * @throws IOException
	 */
	// TODO DBManagerのテストケースに移す
	@Test
	void testGetConnection() throws SQLException, IOException {
		Config config = Config.getConfig();
		Connection conn = DBUtils.getConnection(config);
		assertTrue(conn.isValid(1));

		// コネクションの取得に失敗するケース
		config.url = "bad url";
		RuntimeException e =  assertThrows(RuntimeException.class, () -> DBUtils.getConnection(config));
		assertEquals(SQLException.class, e.getCause().getClass());
	}

	@Test
	void testToDate() {
		assertEquals("2010-01-13", DBUtils.toDate("2010-01-13").toString());
		RuntimeException e =  assertThrows(RuntimeException.class, () -> DBUtils.toDate("Bad String"));
		assertEquals(ParseException.class, e.getCause().getClass());
	}

	@Test
	void testToTimestamp() {
		assertEquals("2010-01-13 18:12:21.999", DBUtils.toTimestamp("2010-01-13 18:12:21.999").toString());
		assertEquals("2010-01-13 18:12:21.0", DBUtils.toTimestamp("2010-01-13 18:12:21.000").toString());
		RuntimeException e =  assertThrows(RuntimeException.class, () -> DBUtils.toTimestamp("Bad String"));
		assertEquals(ParseException.class, e.getCause().getClass());
	}

	@Test
	void testNextDate() {
		assertEquals(DBUtils.toDate("2020-11-11"), DBUtils.nextDate(DBUtils.toDate("2020-11-10")));
	}


	@Test
	void testNextMonth() {
		assertEquals(DBUtils.toDate("2020-12-01"), DBUtils.nextMonth(DBUtils.toDate("2020-11-10")));
		assertEquals(DBUtils.toDate("2021-01-01"), DBUtils.nextMonth(DBUtils.toDate("2020-12-31")));
		assertEquals(DBUtils.toDate("2021-02-01"), DBUtils.nextMonth(DBUtils.toDate("2021-01-01")));
	}

	@Test
	void testPreviousMonthLastDay() {
		assertEquals(DBUtils.toDate("2020-10-31"), DBUtils.previousMonthLastDay(DBUtils.toDate("2020-11-10")));
		assertEquals(DBUtils.toDate("2020-11-30"), DBUtils.previousMonthLastDay(DBUtils.toDate("2020-12-31")));
		assertEquals(DBUtils.toDate("2020-12-31"), DBUtils.previousMonthLastDay(DBUtils.toDate("2021-01-01")));
	}


}
