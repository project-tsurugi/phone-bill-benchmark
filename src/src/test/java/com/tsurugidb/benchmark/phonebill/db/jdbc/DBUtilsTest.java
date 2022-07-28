package com.tsurugidb.benchmark.phonebill.db.jdbc;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.ParseException;

import org.junit.jupiter.api.Test;
import org.postgresql.util.PSQLException;

import com.tsurugidb.benchmark.phonebill.app.Config;

class DBUtilsTest {
	/**
	 * getConnection()のテスト
	 * @throws SQLException
	 * @throws IOException
	 */
	@Test
	void testGetConnection() throws SQLException, IOException {
		Config config = Config.getConfig(new String[0]);
		Connection conn = DBUtils.getConnection(config);
		assertTrue(conn.isValid(1));

		// コネクションの取得に失敗するケース
		config.url = "bad url";
		RuntimeException e =  assertThrows(RuntimeException.class, () -> DBUtils.getConnection(config));
		assertEquals(SQLException.class, e.getCause().getClass());
	}

	/**
	 * toDate()のテスト
	 */
	@Test
	void testToDate() {
		assertEquals("2010-01-13", DBUtils.toDate("2010-01-13").toString());
		RuntimeException e =  assertThrows(RuntimeException.class, () -> DBUtils.toDate("Bad String"));
		assertEquals(ParseException.class, e.getCause().getClass());
	}

	/**
	 * toTimestamp()のテスト
	 */
	@Test
	void testToTimestamp() {
		assertEquals("2010-01-13 18:12:21.999", DBUtils.toTimestamp("2010-01-13 18:12:21.999").toString());
		assertEquals("2010-01-13 18:12:21.0", DBUtils.toTimestamp("2010-01-13 18:12:21.000").toString());
		RuntimeException e =  assertThrows(RuntimeException.class, () -> DBUtils.toTimestamp("Bad String"));
		assertEquals(ParseException.class, e.getCause().getClass());
	}

	/**
	 * nextDate()のテスト
	 */
	@Test
	void testNextDate() {
		assertEquals(DBUtils.toDate("2020-11-11"), DBUtils.nextDate(DBUtils.toDate("2020-11-10")));
	}


	/**
	 * nextDate()のテスト
	 */
	@Test
	void testNextMonth() {
		assertEquals(DBUtils.toDate("2020-12-01"), DBUtils.nextMonth(DBUtils.toDate("2020-11-10")));
		assertEquals(DBUtils.toDate("2021-01-01"), DBUtils.nextMonth(DBUtils.toDate("2020-12-31")));
		assertEquals(DBUtils.toDate("2021-02-01"), DBUtils.nextMonth(DBUtils.toDate("2021-01-01")));
	}

	/**
	 * previousMonthLastDay()のテスト
	 */
	@Test
	void testPreviousMonthLastDay() {
		assertEquals(DBUtils.toDate("2020-10-31"), DBUtils.previousMonthLastDay(DBUtils.toDate("2020-11-10")));
		assertEquals(DBUtils.toDate("2020-11-30"), DBUtils.previousMonthLastDay(DBUtils.toDate("2020-12-31")));
		assertEquals(DBUtils.toDate("2020-12-31"), DBUtils.previousMonthLastDay(DBUtils.toDate("2021-01-01")));
	}


	/**
	 * isRetriableSQLException()のテスト
	 */
	@Test
	void testIsRetriableSQLException() {
		// Ora-8177のケース
		assertTrue(DBUtils.isRetriableSQLException(new ORA8177()));

		// PostgreSQLでserialization_failureが起きたときのケース
		assertTrue(DBUtils.isRetriableSQLException(new SerializationFailureException("40001")));

		// PostgreSQLでserialization_failure以外のExceptionが起きたときのケース
		assertFalse(DBUtils.isRetriableSQLException(new SerializationFailureException("02000")));

		// 上記のいずれでもないケース
		assertFalse(DBUtils.isRetriableSQLException(new SQLException()));

		// BatchUpdateExceptionにラッピングされているケース
		assertTrue(DBUtils.isRetriableSQLException(new BatchUpdateException(new ORA8177())));

		// BatchUpdateExceptionにSQLException以外がラッピングされているケース
		assertFalse(DBUtils.isRetriableSQLException(new BatchUpdateException(new Exception())));

	}


	private static class ORA8177 extends SQLException {
		@Override
		public int getErrorCode() {
			return 8177;
		}
	}

	private static class SerializationFailureException extends PSQLException {
		String errorCode;

		public SerializationFailureException(String errorCode) {
			super("serialization_failure", null);
			this.errorCode = errorCode;
		}

		@Override
		public String getSQLState() {
			return errorCode;
		}
	}
}
