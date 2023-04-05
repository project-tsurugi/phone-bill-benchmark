package com.tsurugidb.benchmark.phonebill.testdata;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;

/**
 * 電話番号、および通話時間に関する統計情報を生成する.
 *
 * 通話履歴のレコードの生成毎に、{@link addHistoy(History h)}を呼び出し、
 * 全レコードの生成完了後に{@link getStatistics()}を呼び出して使用する。
 *
 */
public class Statistics {
	/**
	 * 改行文字
	 */
	private static final String LS = System.lineSeparator();
	/**
	 * 頻度のレポートに出力する項目数
	 */
	private static final int NUMBER_OF_FREQUENCY_ITEMS = 10;
	/**
	 * 料金計算対象の最初の日
	 */
	private long startDate;
	/**
	 * 料金計算対象の最終の日
	 */
	private long endDate;

	/**
	 * 通話履歴のレコード数
	 */
	private long numberOfHistories = 0;

	// キー項目と頻度のカウンターのマップ
	private Map<String, Counter<String>> callerMap = new HashMap<>();
	private Map<String, Counter<String>> recipientMap = new HashMap<>();
	private Map<Integer, Counter<Integer>> callTimeMap = new HashMap<>();
	private Map<String, Counter<String>> targetRecipientMap = new HashMap<>();
	private Map<String, Counter<String>> targetCallerMap = new HashMap<>();
	private Map<String, Counter<String>> targetNumberMap = new HashMap<>();

	public Statistics(Date minDate, Date maxDate) {
		this.startDate = minDate.getTime();
		this.endDate = DateUtils.nextDate(maxDate).getTime();
	}


	/**
	 * 1通話履歴分のデータを加える
	 */
	public void addHistoy(History h) {
		numberOfHistories++;
		increment(callerMap, h.getCallerPhoneNumber());
		increment(recipientMap, h.getRecipientPhoneNumber());
		increment(callTimeMap, h.getTimeSecs());
		long time = h.getStartTime().getTime();
		if (startDate <= time && time < endDate) {
			increment(targetCallerMap, h.getCallerPhoneNumber());
			increment(targetRecipientMap, h.getRecipientPhoneNumber());
			switch (h.getPaymentCategorty()) {
			case "R":
				increment(targetNumberMap, h.getRecipientPhoneNumber());
				break;
			case "C":
				increment(targetNumberMap, h.getCallerPhoneNumber());
				break;
			default:
				throw new AssertionError("Unexpected value: " + h.getPaymentCategorty());
			}
		}
	}

	/**
	 * ソート済み通話時間毎の出現頻度のリストを返す
	 *
	 * @return
	 */
	List<Counter<Integer>> getSortedCallTimeFrequencies() {
		return callTimeMap.values().stream().sorted().collect(Collectors.toList());
	}


	/**
	 * ソート済み発信者電話番号の出現頻度のリストを返す
	 *
	 * @return
	 */
	List<Counter<String>> getSortedCallerPhoneNumberFrequencies() {
		return getSortedList(callerMap);
	}

	/**
	 * ソート済み受信者電話番号の出現頻度のリストを返す
	 *
	 * @return
	 */
	List<Counter<String>> getSortedRecipientPhoneNumberFrequencies() {
		return getSortedList(recipientMap);
	}

	/**
	 * ソート済み料金計算対象の通話の発信者電話番号の出現頻度のリストを返す
	 *
	 * @return
	 */
	public List<Counter<String>> getSortedTargetCallerPhoneNumberFrequencies() {
		return getSortedList(targetCallerMap);
	}

	/**
	 * ソート済み料金計算対象の通話の受信者電話番号の出現頻度のリストを返す
	 *
	 * @return
	 */
	public List<Counter<String>> getSortedTargetRecipientPhoneNumberFrequencies() {
		return getSortedList(targetRecipientMap);
	}

	/**
	 * ソート済み料金計算対象の電話番号の出現頻度のリストを返す
	 *
	 * @return
	 */
	public List<Counter<String>> getSortedTargetPhoneNumberFrequencies() {
		return getSortedList(targetNumberMap);
	}

	private <K extends Comparable<? super K>> List<Counter<K>> getSortedList(Map<String, Counter<K>> map) {
		return map.values().stream().sorted().collect(Collectors.toList());
	}


	/**
	 * 指定のmapのキー値がkeyの値をインクリメントする
	 * @param <T>
	 * @param <T>
	 *
	 * @param <V>
	 * @param map
	 * @param key
	 */
	private static  <K extends Comparable<? super K>> void increment(Map<K, Counter<K>> map, K key) {
		Counter<K> c = map.get(key);
		if (c == null) {
			c = new Counter<K>(key, 0);
			map.put(key, c);
		}
		c.increment();
	}



	/**
	 * 特定のキー値の頻度を保持するカウンター
	 * @param <T1>
	 *
	 */
	public static class Counter<K extends Comparable<? super K>> implements Comparable<Counter<K>>{
		/**
		 * キー値
		 */
		K key;

		/**
		 * キー値の頻度
		 */
		int count;

		void increment() {
			count++;
		}

		/**
		 * コンストラクタ
		 *
		 * @param key
		 * @param count
		 */
		public Counter(K key, int count) {
			this.key = key;
			this.count = count;
		}

		/**
		 * countの降順、countが等しい場合はキーの昇順でソートされるような値を返す
		 */
		@Override
		public int compareTo(Counter<K> o) {
			if (count == o.count) {
				return key.compareTo(o.key);
			}
			return Integer.compare(o.count, count);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + count;
			result = prime * result + ((key == null) ? 0 : key.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Counter<?> other = (Counter<?>) obj;
			if (count != other.count)
				return false;
			if (key == null) {
				if (other.key != null)
					return false;
			} else if (!key.equals(other.key))
				return false;
			return true;
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("Counter [key=");
			builder.append(key);
			builder.append(", count=");
			builder.append(count);
			builder.append("]");
			return builder.toString();
		}
	}



	/**
	 * 統計情報を出力する
	 *
	 * @param path
	 * @throws IOException
	 */
	public String getReport() throws IOException {
		try (
				StringWriter sw = new StringWriter();
				PrintWriter pw = new PrintWriter(sw)) {
			writeSummary(pw);
			pw.flush();
			sw.flush();
			return sw.toString();
		}
	}

	/**
	 * summaryを出力する
	 *
	 * @param pw
	 * @throws IOException
	 */
	private void writeSummary(PrintWriter pw) throws IOException {
		pw.println("statistics report");
		writeStatistics(pw, getSortedCallTimeFrequencies(), "call time", "call time");
		writeStatistics(pw, getSortedCallerPhoneNumberFrequencies(), "Caller phone number", "phone number");
		writeStatistics(pw, getSortedRecipientPhoneNumberFrequencies(), "Recipient phone number", "phone number");
		writeStatistics(pw, getSortedTargetCallerPhoneNumberFrequencies(),
				"caller phone numbers in the history to be calcurated", "phone number");
		writeStatistics(pw, getSortedTargetRecipientPhoneNumberFrequencies(),
				"Recipient phone numbers in the history to be calcurated", "phone number");
		writeStatistics(pw, getSortedTargetPhoneNumberFrequencies(),
				"Phone numbers to be calcurated(equals to contracts to be calcurated)",
				"phone number");
		pw.println("----------------- history information -----------------");
		String format = "number of history records. = %,d";
		pw.println(String.format(format, numberOfHistories));

	}

	private <K extends Comparable<? super K>> void writeStatistics(PrintWriter pw, List<Counter<K>> list,
			String title, String keyName) {
		pw.println("----------------- " + title + " -----------------");
		pw.println("Number of " + keyName + " = " + list.size());
		K max = null;
		K min = null;
		for (Counter<K> c : list) {
			if (max == null) {
				max = c.key;
				min = c.key;
			} else {
				max = (max.compareTo(c.key) > 0) ? max : c.key;
				min = (min.compareTo(c.key) < 0) ? min : c.key;
			}
		}
		pw.println("Maximum " + keyName + " = " + max);
		pw.println("Minimum " + keyName + " = " + min);
		double mean = list.stream().collect(Collectors.averagingDouble(c -> c.count));
		pw.println(String.format("Mean frequency = %.2f", mean));
		pw.println("Hight frequencies");
		pw.print(getFrequenciesReport(list, NUMBER_OF_FREQUENCY_ITEMS, keyName, true));
		pw.println("Low frequencies");
		pw.print(getFrequenciesReport(list, NUMBER_OF_FREQUENCY_ITEMS, keyName, false));
		pw.println();

	}


	/**
	 * ソート済みの頻度のリストからレポートの文字列を作成する.
	 *
	 * <p>listの最初n件、または最後のn件について、頻度と当該頻度のキー値をレポートする
	 *
	 * @param <K> キーの型
	 * @param list 対象のリスト
	 * @param n レポート対象の項目数
	 * @param keyName キーの名称
	 * @param top trueのとき最初のn件、falseのとき最後のn件
	 * @return
	 */
	<K extends Comparable<? super K>> String getFrequenciesReport(List<Counter<K>> list, int n, String keyName, boolean top) {
		int prevCount = -1;
		List<K> keys = new ArrayList<>();
		StringBuilder sb = new StringBuilder();
		for (int i = top ? 0 : (list.size() -1); top ? i < list.size() : i>= 0; i += top ? 1 : -1) {
			Counter<K> c = list.get(i);
			int count = c.count;
			if (count != prevCount) {
				if (prevCount != -1) {
					sb.append(getKeysString(keys, keyName));
					if ((top ? i : list.size() - i - 1) >= n) {
						return sb.toString();
					}
					keys.clear();
				}
				prevCount = count;
				sb.append("    Frequency = " + count + ", ");
			}
			keys.add(c.key);
			if (keys.size() > n) {
				return sb.append("too many " + keyName + "s" + LS).toString();
			}
		}
		return sb.append(getKeysString(keys, keyName)).toString();
	}


	private <K extends Comparable<? super K>> String getKeysString(List<K> keys, String keyName) {
		Collections.sort(keys);
		return keyName + " = " + keys.stream().map(k -> k.toString()).collect(Collectors.joining(", ")) + LS;
	}

}
