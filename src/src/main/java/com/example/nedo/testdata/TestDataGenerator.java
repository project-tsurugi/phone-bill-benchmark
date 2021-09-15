package com.example.nedo.testdata;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.app.Config;
import com.example.nedo.db.Contract;
import com.example.nedo.db.DBUtils;
import com.example.nedo.db.Duration;
import com.example.nedo.db.History;
import com.example.nedo.testdata.GenerateHistoryTask.Params;

public class TestDataGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(TestDataGenerator.class);

	/**
	 * 統計情報
	 */
	private Statistics statistics;

	/**
	 * trueのときにDBに書き込まずデータを生成せず、統計情報を出力する
	 */
	private boolean statisticsOnly;


	/**
	 * コンフィグレーション
	 */
	private Config config;

	/**
	 * 電話番号生成器
	 */
	PhoneNumberGenerator phoneNumberGenerator;


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
	 * オンラインアプリ用のGenerateHistoryTask
	 */
	private GenerateHistoryTask generateHistoryTaskForOnlineApp;


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

	private ContractReader contractReader;


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
		this.contractReader = contractReader;
		phoneNumberGenerator = new PhoneNumberGenerator(config);
		initDurationList();
		if (contractReader == null) {
			contractReader = new ContractReaderImpl();
		}
		this.contractReader = contractReader;
		// オンラインアプリ用のGenerateHistoryTask
		Params params = createTaskParams(0, 0, 0, 0, 1, null).get(0);
		generateHistoryTaskForOnlineApp = new GenerateHistoryTask(params);

		statistics = new Statistics(config.histortyMinDate, config.histortyMaxDate);

	}

	/**
	 * 通話履歴を作成するタスクを作る
	 *
	 * @param start 通話開始時刻の最小値
	 * @param end 通話開始時刻の最大値 + 1
	 * @param writeSize 一度にキューに書き込む履歴数の最大値
	 * @param numbeOfHistory 作成する履歴数
	 * @param numberOfTasks 作成するタスク数
	 * @param queue 書き込むキュー
	 * @return
	 */
	/**
	 * @param start
	 * @param end
	 * @param writeSize
	 * @param numbeOfHistory
	 * @param queue
	 * @return
	 */
	private List<Params> createTaskParams(long start, long end, int writeSize, int numbeOfHistory,
			int numberOfTasks, BlockingQueue<List<History>> queue) {
		Params params = new Params();
		params.taskId = 0;
		params.config = config;
		params.random = random;
		params.contractReader = contractReader;
		params.phoneNumberGenerator = phoneNumberGenerator;
		params.durationList = durationList;
		params.start = start;
		params.end = end;
		params.writeSize = writeSize;
		params.numbeOfHistory = numbeOfHistory;
		params.queue = queue;

		if (numberOfTasks <= 1) {
			return Collections.singletonList(params);
		} else {
			return createParams(params, numberOfTasks);
		}
	}


	/**
	 * 指定のパラメータを元に指定の数にタスクを分割したパラメータを生成する
	 *
	 * @param params
	 * @param numberOfTasks
	 * @return
	 */
	private List<Params> createParams(Params params, int numberOfTasks) {
		List<Params> list = new ArrayList<>(numberOfTasks);

		for(int i = 0; i < numberOfTasks; i++) {
			Params dividedParams = params.clone();
			dividedParams.taskId = i;
			dividedParams.start =params.start + (params.end - params.start) * i / numberOfTasks;
			dividedParams.end =params.start + (params.end - params.start) * (i+1) / numberOfTasks;
			dividedParams.numbeOfHistory = params.numbeOfHistory / numberOfTasks;
			if (i == 0) {
				// dividedParams.numbeOfHistory = params.numbeOfHistory / numberOfTasks で計算すると端数がでるので、
				// i == 0 のときに端数を調整した値を入れる

				dividedParams.numbeOfHistory = params.numbeOfHistory
						- (params.numbeOfHistory / numberOfTasks) * (numberOfTasks - 1);
			} else {
			}
			list.add(dividedParams);
		}
		return list;
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
	 * 契約マスタのテストデータをDBに生成する
	 *
	 * @throws SQLException
	 */
	public void generateContractsToDb() throws SQLException {
		try (Connection conn = DBUtils.getConnection(config);
				Statement stmt = conn.createStatement();
				PreparedStatement ps = conn.prepareStatement(SQL_INSERT_TO_CONTRACT)) {
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
	 * 契約マスタのテストデータのCSVファイルを生成する
	 *
	 * @throws IOException
	 */
	public void generateContractsToCsv(Path dir) throws IOException {
		Path outputPath = dir.resolve("contracts.csv");
		StringBuilder sb = new StringBuilder();
		try (BufferedWriter bw = Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8,
				StandardOpenOption.TRUNCATE_EXISTING,
				StandardOpenOption.CREATE,
				StandardOpenOption.WRITE);) {
			for (long n = 0; n < config.numberOfContractsRecords; n++) {
				Contract c = getContract(n);
				sb.setLength(0);
				sb.append(c.phoneNumber);
				sb.append(',');
				sb.append(c.startDate);
				sb.append(',');
				if (c.endDate != null) {
					sb.append(c.endDate);
				}
				sb.append(',');
				sb.append(c.rule);
				bw.write(sb.toString());
				bw.newLine();
			}
		}
	}


	/**
	 * 通話履歴のテストデータをDBに作成する
	 *
	 * @throws SQLException
	 * @throws IOException
	 */
	public void generateHistoryToDb() throws SQLException, IOException {
		DbHistoryWriter dbHistoryWriter = new DbHistoryWriter();
		dbHistoryWriter.execute();
	}

	/**
	 * 通話履歴のテストデータをCSVファイルに作成する
	 *
	 * @throws IOException
	 * @throws SQLException
	 */
	public void generateHistoryToCsv(Path dir) throws IOException, SQLException {
		Path outputPath = dir.resolve("history.csv");
		CsvHistoryWriter csvHistoryWriter = new CsvHistoryWriter(outputPath);
		csvHistoryWriter.execute();
	}


	/**
	 * 通話履歴をインサートする
	 *
	 * @param conn
	 * @param histories
	 * @throws SQLException
	 */
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
		contract.phoneNumber = phoneNumberGenerator.getPhoneNumber(n);
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
	}

	/**
	 * @return statistics
	 */
	protected Statistics getStatistics() {
		return statistics;
	}

	/**
	 * @param statisticsOnly セットする statisticsOnly
	 */
	protected void setStatisticsOnly(boolean statisticsOnly) {
		this.statisticsOnly = statisticsOnly;
	}


	/**
	 * 指定の開始時刻の通話履歴を作成する(オンラインアプリ用)
	 *
	 * @param startTime
	 * @return
	 */
	public synchronized History createHistoryRecord(long startTime) {
		return generateHistoryTaskForOnlineApp.createHistoryRecord(startTime);
	}


	/**
	 * 通話履歴を書き出す抽象クラス.
	 */
	private abstract class HistoryWriter {
		/**
		 * クリーンナップ処理
		 * @throws IOException
		 * @throws SQLException
		 */
		abstract void cleanup() throws IOException, SQLException;

		/**
		 * 通話履歴を生成するタスクを実行し、生成された通話履歴を書き出す
		 *
		 * @throws IOException
		 * @throws SQLException
		 */
		public void execute() throws IOException, SQLException {
			// 各変数の初期化

			Date minDate = config.histortyMinDate;
			Date maxDate = config.histortyMaxDate;

			statistics = new Statistics(minDate, maxDate);

			if (!isValidDurationList(durationList, minDate, maxDate)) {
				throw new RuntimeException("Invalid duration list.");
			}

			// 通話履歴を生成するタスクとスレッドの生成
			ExecutorService service = Executors.newFixedThreadPool(config.threadCount);

			int numberOfTasks = Math.max((int) (config.numberOfHistoryRecords / 100000), 1);
			BlockingQueue<List<History>> queue = new LinkedBlockingQueue<>(config.threadCount * 2);
			List<Params> paramsList = createTaskParams(minDate.getTime(), maxDate.getTime(), SQL_BATCH_EXEC_SIZE,
					config.numberOfHistoryRecords, numberOfTasks, queue);
			List<Future<?>> futurelist = new ArrayList<>(paramsList.size());
			for (Params params : paramsList) {
				GenerateHistoryTask task = new GenerateHistoryTask(params);
				Future<?> future = service.submit(task);
				futurelist.add(future);
			}
			LOG.info("" + numberOfTasks + " tasks were submitted.");
			service.shutdown();


			// 通話履歴の書き出し
			int numberOfEndTasks = 0;

			// 各タスクが生成した通話履歴をファイルに書き出す
			try {
				while (true) {
					List<History> list = queue.poll();
					if (list == null) {
						try {
							// キューに何も書き込まれていない
							Thread.sleep(1);
							continue;
						} catch (InterruptedException e) {
							throw new RuntimeException(e);
						}
					}
					if (list.isEmpty()) {
						numberOfEndTasks++;
						LOG.info(String.format("%d/%d tasks finished.", numberOfEndTasks, numberOfTasks));
						if (numberOfEndTasks >= numberOfTasks) {
							break;
						}
					}
					for (History h : list) {
						if (statisticsOnly) {
							statistics.addHistoy(h);
						} else {
							write(h);
						}
					}
				}
			} finally {
				cleanup();
			}
			// スレッドの終了状態を調べる
			for (Future<?> future : futurelist) {
				try {
					future.get();
				} catch (InterruptedException | ExecutionException e) {
					throw new RuntimeException(e);
				}
			}
		}

		/**
		 * 1レコード分出力する
		 * @throws IOException
		 * @throws SQLException
		 */
		abstract void write(History h) throws IOException, SQLException;
	}

	private class CsvHistoryWriter extends HistoryWriter {
		private StringBuilder sb;
		private BufferedWriter bw;

		public CsvHistoryWriter(Path outputPath)
				throws IOException {
			this.sb = new StringBuilder();
			this.bw = Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8,
					StandardOpenOption.TRUNCATE_EXISTING,
					StandardOpenOption.CREATE,
					StandardOpenOption.WRITE);
		}

		@Override
		void write(History h) throws IOException {
			sb.setLength(0);
			sb.append(h.callerPhoneNumber);
			sb.append(',');
			sb.append(h.recipientPhoneNumber);
			sb.append(',');
			sb.append(h.paymentCategorty);
			sb.append(',');
			sb.append(h.startTime);
			sb.append(',');
			sb.append(h.timeSecs);
			sb.append(',');
			if (h.charge != null) {
				sb.append(h.charge);
			}
			sb.append(',');
			sb.append(h.df ? 1 : 0);
			bw.write(sb.toString());
			bw.newLine();
		}

		@Override
		void cleanup() throws IOException {
			if (bw != null) {
				bw.close();
			}
		}
	}

	private class DbHistoryWriter extends HistoryWriter {
		List<History> histories = new ArrayList<History>(SQL_BATCH_EXEC_SIZE);
		Connection conn;

		public DbHistoryWriter() {
			conn = DBUtils.getConnection(config);
		}

		@Override
		void cleanup() throws IOException, SQLException {
			if (histories.size() != 0) {
				insrtHistories(conn, histories);
			}
			if (conn != null && !conn.isClosed()) {
				conn.close();
			}
		}

		@Override
		void write(History h) throws IOException, SQLException {
			histories.add(h);
			if (histories.size() >= SQL_BATCH_EXEC_SIZE) {
				insrtHistories(conn, histories);
				histories.clear();
			}

		}
	}
}
