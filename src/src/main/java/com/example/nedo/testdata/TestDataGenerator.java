package com.example.nedo.testdata;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.example.nedo.app.Config;
import com.example.nedo.db.Contract;
import com.example.nedo.db.DBUtils;
import com.example.nedo.db.Duration;
import com.example.nedo.db.History;

public class TestDataGenerator {
	private Config config;

	/**
	 * 同じ発信時刻のデータを作らないための作成済みのHistoryDataの発信時刻を記録するSet
	 */
	private Set<Long> startTimeSet;

	/**
	 * 11桁の電話番号をLONG値で表したときの最大値
	 */
	private static final long MAX_PHNE_NUMBER = 99999999999L;

	/**
	 * 一度にインサートする行数
	 */
	private static final int SQL_BATCH_EXEC_SIZE = 300000;

	/**
	 * 契約マスタにレコードをインサートするためのSQL
	 */
	public static final String SQL_INSERT_TO_CONTRACT = "insert into contracts("
			+ "phone_number,"
			+ "start_date,"
			+ "end_date,"
			+ "charge_rule"
			+ ") values(?, ?, ?, ?)";


	/**
	 * 契約期間のパターンを記録するリスト
	 */
	private List<Duration> durationList = new ArrayList<>();


	/**
	 * 乱数生成器
	 */
	private Random random;

	/**
	 * 通話時間生成器
	 */
	private CallTimeGenerator callTimeGenerator;

	/**
	 * 発信者電話番号のSelector
	 */
	private PhoneNumberSelector callerPhoneNumberSelector;

	/**
	 * 受信者電話番号のSelector
	 */
	private PhoneNumberSelector recipientPhoneNumberSelector;




	/**
	 * テストデータ生成のためのパラメータを指定してContractsGeneratorのインスタンスを生成する.
	 *
	 * @param config
	 */
	public TestDataGenerator(Config config) {
		this(config, config.randomSeed);
	}

	/**
	 * 乱数のシードを指定可能なコンストラクタ
	 *
	 * @param config
	 * @param random
	 */
	public TestDataGenerator(Config config, int seed) {
		this(config, config.randomSeed, null);
	}

	/**
	 * 乱数のシードとContractReaderを指定可能なコンストラクタ。ContractReaderにnullが
	 * 指定された場合は、デフォルトのContractReaderを使用する。
	 *
	 * @param config
	 * @param seed
	 * @param contractReader
	 */
	public TestDataGenerator(Config config, int seed, ContractReader contractReader) {
		this.config = config;
		if (config.minDate.getTime() >= config.maxDate.getTime()) {
			throw new RuntimeException("maxDate is less than or equal to minDate, minDate =" + config.minDate + ", maxDate = "
					+ config.maxDate);
		}
		this.random = new Random(seed);
		this.startTimeSet = new HashSet<Long>(config.numberOfHistoryRecords);
		callTimeGenerator = CallTimeGenerator.createCallTimeGenerator(random, config);
		initDurationList();
		if (contractReader == null) {
			contractReader = new ContractReaderImpl();
		}
		callerPhoneNumberSelector = PhoneNumberSelector.createSelector(random,
				config.callerPhoneNumberDistribution,
				config.callerPhoneNumberScale,
				config.callerPhoneNumberShape, contractReader, durationList.size());
		recipientPhoneNumberSelector = PhoneNumberSelector.createSelector(random,
				config.recipientPhoneNumberDistribution,
				config.recipientPhoneNumberScale,
				config.recipientPhoneNumberShape, contractReader, durationList.size());
	}


	/**
	 * 契約マスタのテストデータを生成する
	 *
	 * @throws SQLException
	 */
	public void generateContracts() throws SQLException {
		try (Connection conn = DBUtils.getConnection(config);
				Statement stmt = conn.createStatement();
				PreparedStatement ps = conn.prepareStatement(SQL_INSERT_TO_CONTRACT)) {
			stmt.executeUpdate("truncate table contracts");

			int batchSize = 0;
			for (long n = 0; n < config.numberOfContractsRecords; n++) {
				setContract(ps, n);
				ps.addBatch();
				if (++batchSize == SQL_BATCH_EXEC_SIZE) {
					execBatch(ps);
					batchSize = 0;
				}
			}
			execBatch(ps);
		}
	}

	/**
	 * psにn番目の契約レコードの値をセットする
	 *
	 * @return セットした契約レコード
	 * @throws SQLException
	 */
	public Contract setContract(PreparedStatement ps, long n) throws SQLException {
		Contract c = getContract(n);
		ps.setString(1, c.phoneNumber);
		ps.setDate(2, c.startDate);
		ps.setDate(3, c.endDate);
		ps.setString(4, c.rule);
		return c;
	}

	/**
	 * 二つの期間に共通の期間を返す
	 *
	 * @param d1
	 * @param d2
	 * @return 共通な期間、共通な期間がない場合nullを返す。
	 */
	public static Duration getCommonDuration(Duration d1, Duration d2) {
		// d1, d2に共通な期間がない場合
		if (d1.end.getTime() < d2.start.getTime()) {
			return null;
		}
		if (d2.end.getTime() < d1.start.getTime()) {
			return null;
		}
		if (d1.start.getTime() < d2.start.getTime()) {
			if (d1.end.getTime() < d2.end.getTime()) {
				return new Duration(d2.start, d1.end);
			} else {
				return d2;
			}
		} else {
			if (d1.end.getTime() < d2.end.getTime()) {
				return d1;
			} else {
				return new Duration(d1.start, d2.end);
			}
		}
	}

	/**
	 * 通話履歴のテストデータを作成する
	 *
	 * 生成する通話履歴の通話開始時刻は、minDate以上、maxDate未満の値にする。
	 *
	 * @param minDate
	 * @param maxDate
	 * @param n 生成するレコード数
	 * @throws SQLException
	 */
	public void generateHistory(Date minDate, Date maxDate, int n) throws SQLException {
		if (!isValidDurationList(durationList, minDate, maxDate)) {
			throw new RuntimeException("Invalid duration list.");
		}

		Duration targetDuration = new Duration(minDate, maxDate);
		List<History> histories = new ArrayList<History>(SQL_BATCH_EXEC_SIZE);

		try (Connection conn = DBUtils.getConnection(config)) {
			for (int i = 0; i < n; i++) {
				histories.add(createHistoryRecord(targetDuration));
				if (histories.size() >= SQL_BATCH_EXEC_SIZE) {
					insrtHistories(conn, histories);
					histories.clear();
				}
			}
			insrtHistories(conn, histories);
		}
	}


	public void insrtHistories(Connection conn, List<History> histories) throws SQLException {
		try (PreparedStatement ps = conn.prepareStatement("insert into history("
				+ "caller_phone_number,"
				+ "recipient_phone_number,"
				+ "payment_categorty,"
				+ "start_time,"
				+ "time_secs,"
				+ "charge,"
				+ "df"
				+ ") values(?, ?, ?, ?, ?, ?, ? )")) {
			for (History h : histories) {
				ps.setString(1, h.callerPhoneNumber);
				ps.setString(2, h.recipientPhoneNumber);
				ps.setString(3, h.paymentCategorty);
				ps.setTimestamp(4, h.startTime);
				ps.setInt(5, h.timeSecs);
				if (h.charge == null) {
					ps.setNull(6, Types.INTEGER);
				} else {
					ps.setInt(6, h.charge);
				}
				ps.setInt(7, h.df ? 1 : 0);
				ps.addBatch();
			}
			execBatch(ps);
		}
	}

	/**
	 * minDate～maxDateの間の全ての日付に対して、当該日付を含むdurationがlistに二つ以上あることを確認する
	 *
	 * @param list
	 * @param minDate
	 * @param maxDate
	 */
	static boolean isValidDurationList(List<Duration> list, Date minDate, Date maxDate) {
		if (minDate.getTime() > maxDate.getTime()) {
			return false;
		}
		for (Date date = minDate; date.getTime() <= maxDate.getTime(); date = DBUtils.nextDate(date)) {
			int c = 0;
			for (Duration duration : list) {
				long start = duration.start.getTime();
				long end = duration.end == null ? Long.MAX_VALUE : duration.end.getTime();
				if (start <= date.getTime() && date.getTime() <= end) {
					c++;
					if (c >= 2) {
						break;
					}
				}
			}
			if (c < 2) {
				System.err.println("Duration List not contains date: " + date);
				return false;
			}
		}
		return true;
	}

	/**
	 * 通話開始時刻が指定の範囲に収まる通話履歴を生成する
	 *
	 * @param targetDuration
	 * @return
	 */
	private History createHistoryRecord(Duration targetDuration) {
		// 通話開始時刻
		long startTime;
		do {
			startTime = TestDataUtils.getRandomLong(random, targetDuration.start.getTime(), targetDuration.end.getTime());
		} while (startTimeSet.contains(startTime));
		startTimeSet.add(startTime);
		return createHistoryRecord(startTime);
	}

	/**
	 * 指定の通話開始時刻の通話履歴を生成する
	 *
	 * @param startTime
	 * @return
	 */
	public History createHistoryRecord(long startTime) {
		History history = new History();
		history.startTime = new Timestamp(startTime);

		// 電話番号の生成
		history.callerPhoneNumber = callerPhoneNumberSelector.selectPhoneNumber(startTime, null);
		history.recipientPhoneNumber = recipientPhoneNumberSelector.selectPhoneNumber(startTime, history.callerPhoneNumber);

		// 料金区分(発信者負担、受信社負担)
		// TODO 割合を指定可能にする
		history.paymentCategorty = random.nextInt(2) == 0 ? "C" : "R";

		// 通話時間
		history.timeSecs = callTimeGenerator.getTimeSecs();

		return history;
	}

	private void execBatch(PreparedStatement ps) throws SQLException {
		int rets[] = ps.executeBatch();
		for (int ret : rets) {
			if (ret < 0 && ret != PreparedStatement.SUCCESS_NO_INFO) {
				throw new SQLException("Fail to batch exexecute");
			}
		}
		ps.getConnection().commit();
		ps.clearBatch();
	}

	/**
	 * 契約日のパターンのリストを作成する
	 */
	private void initDurationList() {
		// TODO: もっとバリエーションが欲しい
		// 契約終了日がないduration
		for (int i = 0; i < config.noExpirationDateRate; i++) {
			Date start = getDate(config.minDate, config.maxDate);
			durationList.add(new Duration(start, null));
		}
		// 契約終了日があるduration
		for (int i = 0; i < config.expirationDateRate; i++) {
			Date start = getDate(config.minDate, config.maxDate);
			Date end = getDate(start, config.maxDate);
			durationList.add(new Duration(start, end));
		}
		// 同一電話番号の契約が複数あるパターン用のduration
		for (int i = 0; i < config.duplicatePhoneNumberRatio; i++) {
			Date end = getDate(config.minDate, DBUtils.previousMonthLastDay(config.maxDate));
			Date start = getDate(DBUtils.nextMonth(end), config.maxDate);
			durationList.add(new Duration(config.minDate, end));
			durationList.add(new Duration(start, null));
		}
	}

	/**
	 * durationListを取得する(UT用)
	 *
	 * @return durationList
	 */
	List<Duration> getDurationList() {
		return durationList;
	}

	/**
	 * min～maxの範囲のランダムな日付を取得する
	 *
	 * @param min
	 * @param max
	 * @return
	 */
	Date getDate(Date min, Date max) {
		int days = (int) ((max.getTime() - min.getTime()) / DBUtils.A_DAY_IN_MILLISECONDS);
		long offset = random.nextInt(days + 1) * DBUtils.A_DAY_IN_MILLISECONDS;
		return new Date(min.getTime() + offset);
	}

	/**
	 * n番目の電話番号(11桁)を返す
	 *
	 * @param n
	 * @return
	 */
	String getPhoneNumber(long n) {
		if (n < 0 || MAX_PHNE_NUMBER <= n) {
			throw new RuntimeException("Out of phone number range: " + n);
		}
		long blockSize = config.duplicatePhoneNumberRatio * 2 + config.expirationDateRate + config.noExpirationDateRate;
		long noDupSize = config.expirationDateRate + config.noExpirationDateRate;
		long posInBlock = n % blockSize;
		long phoneNumber = n;
		if (posInBlock >= noDupSize && posInBlock % 2 == 0) {
			phoneNumber = n + 1;
		}
		String format = "%011d";
		return String.format(format, phoneNumber);
	}

	/**
	 * n番目のレコードのDurationを返す
	 *
	 * @param n
	 * @return
	 */
	Duration getDuration(long n) {
		return durationList.get((int) (n % durationList.size()));
	}


	/**
	 * n番目の契約レコードを返す
	 *
	 * @param n
	 * @return
	 */
	private Contract getContract(long n) {
		Contract contract = new Contract();
		contract.phoneNumber = getPhoneNumber(n);
		contract.rule = "sample";
		Duration d = getDuration(n);
		contract.startDate = d.start;
		contract.endDate = d.end;
		return contract;
	}


	/**
	 * 本クラスが保持する情報を用いて情報を取得するContractReader
	 *
	 */
	class ContractReaderImpl implements ContractReader {
		@Override
		public int getNumberOfContracts() {
			return config.numberOfContractsRecords;
		}

		@Override
		public Duration getDurationByPos(int n) {
			return getDuration(n);
		}

		@Override
		public String getPhoneNumberByPos(int n) {
			return getPhoneNumber(n);
		}
	}
}


