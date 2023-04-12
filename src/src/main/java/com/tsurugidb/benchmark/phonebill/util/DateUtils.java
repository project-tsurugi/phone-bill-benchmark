package com.tsurugidb.benchmark.phonebill.util;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public interface DateUtils {
	/**
	 * ミリ秒で表した1日
	 */
	public static final long A_DAY_IN_MILLISECONDS = 24 * 3600 * 1000;

	static  DateTimeFormatter DF_DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");
	static  DateTimeFormatter DF_TIMESTAMP = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

	static Date toDate(String date) {
		LocalDate ld = LocalDate.parse(date, DF_DATE);
		return Date.valueOf(ld);
	}

	static Timestamp toTimestamp(String date) {
		LocalDateTime ldt = LocalDateTime.parse(date, DF_TIMESTAMP);
		return Timestamp.valueOf(ldt);
	}


	/**
	 * Epocミリ秒をLocalDateTimeに変換する
	 *
	 * @param time
	 * @return
	 */
	static LocalDateTime toLocalDateTime(long time) {
		return LocalDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneId.systemDefault());
	}

	/**
	 * Epocミリ秒をLocalDateに変換する
	 *
	 * @param time
	 * @return
	 */
	static LocalDate toLocalDate(long time) {
		return LocalDate.ofInstant(Instant.ofEpochMilli(time), ZoneId.systemDefault());
	}

	/**
	 * LocalDateTimeをEpocミリ秒に変換する
	 *
	 * @param localDateTime
	 * @return
	 */
	static long toEpocMills(LocalDateTime localDateTime) {
		return Timestamp.valueOf(localDateTime).getTime();
	}

	/**
	 * LocalDateをミリ秒に変換する
	 *
	 * @param localDate
	 * @return
	 */
	static long toEpocMills(LocalDate localDate) {
		return Date.valueOf(localDate).getTime();
	}


	/**
	 * 指定のdateの次の日を返す
	 *
	 * @param date
	 * @return
	 */
	public static Date nextDate(Date date) {
		LocalDate localDate = date.toLocalDate();
		localDate = localDate.plusDays(1);
		return Date.valueOf(localDate);
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
}
