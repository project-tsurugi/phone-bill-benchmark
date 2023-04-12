package com.tsurugidb.benchmark.phonebill.testdata;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.benchmark.phonebill.testdata.Statistics.Counter;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;

class StatisticsTest {
	private static final String LS = System.lineSeparator();

	@Test
	void test() throws IOException {
		Statistics s = new Statistics(DateUtils.toDate("2020-10-31"), DateUtils.toDate("2020-11-01"));
		s.addHistoy(createHistory(1, 5, "2020-10-30 23:59:59.999", 10, "C"));
		s.addHistoy(createHistory(2, 5, "2020-10-31 00:00:00.000", 10, "C"));
		s.addHistoy(createHistory(31, 8, "2020-10-31 23:59:59.999", 1, "C"));
		s.addHistoy(createHistory(1, 4, "2020-10-31 23:59:59.999", 3, "R"));
		s.addHistoy(createHistory(5, 3, "2020-10-31 23:59:59.999", 10, "C"));
		s.addHistoy(createHistory(6, 1, "2020-10-31 23:59:59.999", 6, "C"));
		s.addHistoy(createHistory(8, 2, "2020-10-31 23:59:59.999", 8, "C"));
		s.addHistoy(createHistory(1, 9, "2020-11-01 23:59:59.999", 4, "C"));
		s.addHistoy(createHistory(2, 5, "2020-11-02 00:00:00.000", 3, "R"));

		testGetSortedCallTimeFrequencies(s);
		testGetSortedCallerPhoneNumberFrequencies(s);
		testGetSortedRecipientPhoneNumberFrequencies(s);
		testGetSortedTargetCallerPhoneNumberFrequencies(s);
		testGetSortedTargetRecipientPhoneNumberFrequencies(s);
		testGetSortedTargetPhoneNumberFrequencies(s);

		System.out.println(s.getReport());
	}

	private void testGetSortedCallTimeFrequencies(Statistics s) {
		List<Counter<Integer>> list = s.getSortedCallTimeFrequencies();
		assertEquals(6, list.size());
		int i = 0;
		assertEquals(new Counter<Integer>(10, 3), list.get(i++));
		assertEquals(new Counter<Integer>(3, 2), list.get(i++));
		assertEquals(new Counter<Integer>(1, 1), list.get(i++));
		assertEquals(new Counter<Integer>(4, 1), list.get(i++));
		assertEquals(new Counter<Integer>(6, 1), list.get(i++));
		assertEquals(new Counter<Integer>(8, 1), list.get(i++));
	}

	private void testGetSortedCallerPhoneNumberFrequencies(Statistics s) {
		List<Counter<String>> list = s.getSortedCallerPhoneNumberFrequencies();
		assertEquals(6, list.size());
		int i = 0;
		assertEquals(new Counter<String>("0001", 3), list.get(i++));
		assertEquals(new Counter<String>("0002", 2), list.get(i++));
		assertEquals(new Counter<String>("0005", 1), list.get(i++));
		assertEquals(new Counter<String>("0006", 1), list.get(i++));
		assertEquals(new Counter<String>("0008", 1), list.get(i++));
		assertEquals(new Counter<String>("0031", 1), list.get(i++));
	}

	private void testGetSortedRecipientPhoneNumberFrequencies(Statistics s) {
		List<Counter<String>> list = s.getSortedRecipientPhoneNumberFrequencies();
		assertEquals(7, list.size());
		int i = 0;
		assertEquals(new Counter<String>("0005", 3), list.get(i++));
		assertEquals(new Counter<String>("0001", 1), list.get(i++));
		assertEquals(new Counter<String>("0002", 1), list.get(i++));
		assertEquals(new Counter<String>("0003", 1), list.get(i++));
		assertEquals(new Counter<String>("0004", 1), list.get(i++));
		assertEquals(new Counter<String>("0008", 1), list.get(i++));
		assertEquals(new Counter<String>("0009", 1), list.get(i++));
	}

	private void testGetSortedTargetCallerPhoneNumberFrequencies(Statistics s) {
		List<Counter<String>> list = s.getSortedTargetCallerPhoneNumberFrequencies();
		assertEquals(6, list.size());
		int i = 0;
		assertEquals(new Counter<String>("0001", 2), list.get(i++));
		assertEquals(new Counter<String>("0002", 1), list.get(i++));
		assertEquals(new Counter<String>("0005", 1), list.get(i++));
		assertEquals(new Counter<String>("0006", 1), list.get(i++));
		assertEquals(new Counter<String>("0008", 1), list.get(i++));
		assertEquals(new Counter<String>("0031", 1), list.get(i++));
	}

	private void testGetSortedTargetRecipientPhoneNumberFrequencies(Statistics s) {
		List<Counter<String>> list = s.getSortedTargetRecipientPhoneNumberFrequencies();
		assertEquals(7, list.size());
		int i = 0;
		assertEquals(new Counter<String>("0001", 1), list.get(i++));
		assertEquals(new Counter<String>("0002", 1), list.get(i++));
		assertEquals(new Counter<String>("0003", 1), list.get(i++));
		assertEquals(new Counter<String>("0004", 1), list.get(i++));
		assertEquals(new Counter<String>("0005", 1), list.get(i++));
		assertEquals(new Counter<String>("0008", 1), list.get(i++));
		assertEquals(new Counter<String>("0009", 1), list.get(i++));
	}

	private void testGetSortedTargetPhoneNumberFrequencies(Statistics s) {
		List<Counter<String>> list = s.getSortedTargetPhoneNumberFrequencies();
		assertEquals(7, list.size());
		int i = 0;
		assertEquals(new Counter<String>("0001", 1), list.get(i++));
		assertEquals(new Counter<String>("0002", 1), list.get(i++));
		assertEquals(new Counter<String>("0004", 1), list.get(i++));
		assertEquals(new Counter<String>("0005", 1), list.get(i++));
		assertEquals(new Counter<String>("0006", 1), list.get(i++));
		assertEquals(new Counter<String>("0008", 1), list.get(i++));
		assertEquals(new Counter<String>("0031", 1), list.get(i++));
	}

	/**
	 * 指定の値で通話履歴を作成する
	 *
	 * @param callerPhoneNumber 発信者電話番号を表す整数値
	 * @param recipientPhoneNumber 受信者電話番号を表す整数値
	 * @param startTime 通話開始時刻を表す文字列
	 * @param timeSec 通話時間
	 * @return
	 */
	private History createHistory(int callerPhoneNumber, int recipientPhoneNumber, String startTime, int timeSec, String paymentCategorty) {
		History h = new History();
		h.setCallerPhoneNumber(toPhoneNumber(callerPhoneNumber));
		h.setRecipientPhoneNumber(toPhoneNumber(recipientPhoneNumber));
		h.setTimeSecs(timeSec);
		h.setStartTime(DateUtils.toTimestamp(startTime));
		h.setPaymentCategorty(paymentCategorty);
		return h;
	}

	/**
	 * 整数値から電話番号を示す文字列を生成する
	 *
	 * @param phoneNumber
	 * @return
	 */
	private String toPhoneNumber(int phoneNumber) {
		String format = "%04d";
		return String.format(format, phoneNumber);
	}

	/**
	 * getFrequenciesReport()のテスト
	 */
	@Test
	void testGetFrequenciesReport() {
		Date dummy = new Date(System.currentTimeMillis());
		Statistics s = new Statistics(dummy, dummy);

		List<Counter<String>> list = new ArrayList<>();
		list.add(new Counter<String>("K1", 128));
		list.add(new Counter<String>("K2", 18));
		list.add(new Counter<String>("K3", 18));
		list.add(new Counter<String>("K0", 5));
		list.add(new Counter<String>("K5", 3));

		String expected =
				"    Frequency = 128, MyKey = K1" + LS	+
				"    Frequency = 18, MyKey = K2, K3" + LS;
		assertEquals(expected, s.getFrequenciesReport(list, 3, "MyKey", true));
		assertEquals(expected, s.getFrequenciesReport(list, 2, "MyKey", true));

		expected = expected +
				"    Frequency = 5, MyKey = K0" + LS	+
				"    Frequency = 3, MyKey = K5" + LS;
		assertEquals(expected, s.getFrequenciesReport(list, 5, "MyKey", true));
		assertEquals(expected, s.getFrequenciesReport(list, 99, "MyKey", true));


		expected =
				"    Frequency = 3, MyKey = K5" + LS	+
				"    Frequency = 5, MyKey = K0" + LS	+
				"    Frequency = 18, MyKey = K2, K3" + LS;
		assertEquals(expected, s.getFrequenciesReport(list, 3, "MyKey", false));
		assertEquals(expected, s.getFrequenciesReport(list, 4, "MyKey", false));

		expected = expected +
				"    Frequency = 128, MyKey = K1" + LS;
		assertEquals(expected, s.getFrequenciesReport(list, 5, "MyKey", false));
		assertEquals(expected, s.getFrequenciesReport(list, 99, "MyKey", false));

		list.clear();
		list.add(new Counter<String>("K1", 128));
		list.add(new Counter<String>("K2", 128));
		list.add(new Counter<String>("K3", 128));
		list.add(new Counter<String>("K0", 128));
		list.add(new Counter<String>("K5", 128));
		assertEquals("    Frequency = 128, too many MyKeys" + LS, s.getFrequenciesReport(list, 1, "MyKey", true));
		assertEquals("    Frequency = 128, too many MyKeys" + LS, s.getFrequenciesReport(list, 4, "MyKey", true));
		assertEquals("    Frequency = 128, MyKey = K0, K1, K2, K3, K5" + LS, s.getFrequenciesReport(list, 5, "MyKey", true));
		assertEquals("    Frequency = 128, MyKey = K0, K1, K2, K3, K5" + LS, s.getFrequenciesReport(list, 6, "MyKey", true));
		assertEquals("    Frequency = 128, too many MyKeys" + LS, s.getFrequenciesReport(list, 1, "MyKey", false));
		assertEquals("    Frequency = 128, too many MyKeys" + LS, s.getFrequenciesReport(list, 4, "MyKey", false));
		assertEquals("    Frequency = 128, MyKey = K0, K1, K2, K3, K5" + LS, s.getFrequenciesReport(list, 5, "MyKey", false));
		assertEquals("    Frequency = 128, MyKey = K0, K1, K2, K3, K5" + LS, s.getFrequenciesReport(list, 6, "MyKey", false));
	}
}


