package com.example.nedo.db;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;

import org.postgresql.util.PSQLException;

import com.example.nedo.app.Config;

public class DBUtils {
	/**
	 * ミリ秒で表した1日
	 */
	public static final long A_DAY_IN_MILLISECONDS = 24 * 3600 * 1000;

	private static  DateFormat DF_DATE = new SimpleDateFormat("yyyy-MM-dd");
	private static  DateFormat DF_TIMESTAMP = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	public static Connection getConnection(Config config) {
        Connection conn;
		try {
			conn = DriverManager.getConnection(config.url, config.user, config.password);
			conn.setAutoCommit(false);
			conn.setTransactionIsolation(config.isolationLevel);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return conn;
	}

	public static synchronized Date toDate(String date) {
		try {
			return new Date(DF_DATE.parse(date).getTime());
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}

	public static synchronized Timestamp toTimestamp(String date) {
		try {
			return new Timestamp(DF_TIMESTAMP.parse(date).getTime());
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * 指定のdateの次の日を返す
	 *
	 * @param date
	 * @return
	 */
	public static Date nextDate(Date date) {
		return new Date(date.getTime() + DBUtils.A_DAY_IN_MILLISECONDS);
	}

	/**
	 * 指定のdateの次の月の１日を返す
	 *
	 * @param date
	 * @return
	 */
	public static Date nextMonth(Date date) {
		LocalDate localDate = date.toLocalDate();
		localDate = localDate.withDayOfMonth(1);
		localDate = localDate.plusMonths(1);
		return Date.valueOf(localDate);
	}

	/**
	 * 指定のdateの前の月の最終日を返す
	 *
	 * @param date
	 * @return
	 */
	public static Date previousMonthLastDay(Date date) {
		LocalDate localDate = date.toLocalDate();
		localDate = localDate.withDayOfMonth(1);
		localDate = localDate.minusDays(1);
		return Date.valueOf(localDate);
	}

	/**
	 * 引数で指定されたSQLExceptionが、リトライすべきものか調べる.
	 *
	 * @param e
	 * @return リトライすべき場合true
	 */
	public static boolean isRetriableSQLException(SQLException e) {
		Throwable t = e instanceof BatchUpdateException ? e.getCause() : e;
		// for PostgreSQL
		if (t instanceof PSQLException && ((PSQLException) t).getSQLState().equals("40001")) {
			return true;
		}
		/// ORA-8177のケース TODO: DBMSがOracleかどうかのチェックも必要
		if (e.getErrorCode() == 8177) {
			return true;
		}

		return false;
	}

}
