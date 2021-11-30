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
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.app.Config;
import com.example.nedo.db.Contract;
import com.example.nedo.db.DBUtils;
import com.example.nedo.db.Duration;
import com.example.nedo.db.History;
import com.example.nedo.testdata.GenerateHistoryTask.Params;
import com.example.nedo.testdata.GenerateHistoryTask.Result;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

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
	private PhoneNumberGenerator phoneNumberGenerator;


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
	 * 乱数生成器
	 */
	private Random random;

	/**
	 * 契約マスタの情報
	 */
	private ContractInfoReader contractInfoReader;


	/**
	 * 契約のブロックに関する情報にアクセスするためのアクセサ
	 */
	private ContractBlockInfoAccessor accessor;

	/**
	 * 乱数のシードとaccessorを指定可能なコンストラクタ。accessorにnullが
	 * 指定された場合は、デフォルトのSingleProcessContractBlockManagerを使用する。
	 *
	 * @param config
	 * @param seed
	 * @param accessor
	 * @throws IOException
	 */
	public TestDataGenerator(Config config, Random random, ContractBlockInfoAccessor accessor) throws IOException {
		this.config = config;
		if (config.minDate.getTime() >= config.maxDate.getTime()) {
			throw new RuntimeException("maxDate is less than or equal to minDate, minDate =" + config.minDate + ", maxDate = "
					+ config.maxDate);
		}
		this.random = random;
		phoneNumberGenerator = new PhoneNumberGenerator(config);
		this.accessor = accessor;
		this.contractInfoReader = ContractInfoReader.create(config, accessor, random);
		statistics = new Statistics(config.historyMinDate, config.historyMaxDate);
	}


	/**
	 * オンラインアプリ用のGenerateHistoryTaskを生成する
	 *
	 * @return
	 * @throws IOException
	 */
	public GenerateHistoryTask getGenerateHistoryTaskForOnlineApp() throws IOException {
		Params params = createTaskParams(0, 0, 0,1).get(0);
		params.historyWriter = new DummyHistoryWriter();
		GenerateHistoryTask generateHistoryTask = new GenerateHistoryTask(params);
		generateHistoryTask.init();
		return generateHistoryTask;
	}

	/**
	 * @param start 通話開始時刻の最小値
	 * @param end 通話開始時刻の最大値 + 1
	 * @param numbeOfHistory 作成する履歴数
	 * @param numberOfTasks 作成するタスク数
	 * @return
	 */
	private List<Params> createTaskParams(long start, long end, long numbeOfHistory,
			int numberOfTasks) {
		Params params = new Params();
		params.taskId = 0;
		params.config = config;
		params.random = new Random(random.nextLong());
		params.accessor = accessor;
		params.phoneNumberGenerator = phoneNumberGenerator;
		params.start = start;
		params.end = end;
		params.numberOfHistory = numbeOfHistory;

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

		long firstNumberOfHistory = 0;
		for(int i = 0; i < numberOfTasks; i++) {
			Params dividedParams = params.clone();
			dividedParams.taskId = i;
			if (i == 0) {
				dividedParams.start = config.historyMinDate.getTime();
				// dividedParams.numbeOfHistory = params.numbeOfHistory / numberOfTasks で計算すると端数がでるので、
				// i == 0 のときに端数を調整した値を入れる
				firstNumberOfHistory = config.numberOfHistoryRecords
						- ((long)config.maxNumberOfLinesHistoryCsv) * (numberOfTasks - 1);
				dividedParams.numberOfHistory = firstNumberOfHistory;
			} else {
				dividedParams.numberOfHistory = config.maxNumberOfLinesHistoryCsv;
				// 各タスクに異なる乱数発生器を使用する
				params.random = new Random(random.nextLong());
				dividedParams.start = list.get(i - 1).end;
			}
			double scale = ((double)firstNumberOfHistory + (double)(config.maxNumberOfLinesHistoryCsv) * i)
			/ (double)(config.numberOfHistoryRecords);
			dividedParams.end = params.start + Math.round((params.end - params.start) * scale);
			list.add(dividedParams);
		}
		return list;
	}

	/**
	 * 新規契約レコードの値をセットする
	 *
	 * @return セットした契約レコード
	 * @throws SQLException
	 * @throws IOException
	 */
	public void setContract(PreparedStatement ps, Contract c) throws SQLException, IOException {
		ps.setString(1, c.phoneNumber);
		ps.setDate(2, c.startDate);
		ps.setDate(3, c.endDate);
		ps.setString(4, c.rule);
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
		if (d1.end < d2.start) {
			return null;
		}
		if (d2.end < d1.start) {
			return null;
		}
		if (d1.start < d2.start) {
			if (d1.end < d2.end) {
				return new Duration(d2.start, d1.end);
			} else {
				return d2;
			}
		} else {
			if (d1.end < d2.end) {
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
	 * @throws IOException
	 */
	public void generateContractsToDb() throws SQLException, IOException {
		try (Connection conn = DBUtils.getConnection(config);
				Statement stmt = conn.createStatement();
				PreparedStatement ps = conn.prepareStatement(SQL_INSERT_TO_CONTRACT)) {
			int batchSize = 0;
			for (long n = 0; n < config.numberOfContractsRecords; n++) {
				Contract c = getNewContract();
				setContract(ps, c);
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
	 * @return
	 * @throws IOException
	 */
	public Contract getNewContract() throws IOException {
		Contract c = contractInfoReader.getNewContract();
		return c;
	}

	/**
	 * 契約マスタのテストデータのCSVファイルを生成する
	 *
	 * @throws IOException
	 */
	public void generateContractsToCsv(Path path) throws IOException {
		StringBuilder sb = new StringBuilder();
		try (BufferedWriter bw = Files.newBufferedWriter(path, StandardCharsets.UTF_8,
				StandardOpenOption.TRUNCATE_EXISTING,
				StandardOpenOption.CREATE,
				StandardOpenOption.WRITE);) {
			for (long n = 0; n < config.numberOfContractsRecords; n++) {
				Contract c = getNewContract();
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
		List<Params> paramsList = createParamsList();
		for(Params params: paramsList) {
			params.historyWriter = new DbHistoryWriter();
		}
		generateHistory(paramsList);
	}

	/**
	 * 通話履歴のテストデータをCSVファイルに作成する
	 *
	 * @throws IOException
	 * @throws SQLException
	 */
	public void generateHistoryToCsv(Path dir) throws IOException, SQLException {
		List<Params> paramsList = createParamsList();
		for(Params params: paramsList) {
			Path outputPath = CsvUtils.getHistortyFilePath(dir, params.taskId);
			params.historyWriter = new CsvHistoryWriter(outputPath);
			LOG.info("task id = {}, start = {}, end = {}, number of history = {}", params.taskId, new Timestamp(params.start), new Timestamp(params.end), params.numberOfHistory);
		}
		generateHistory(paramsList);
	}

	/**
	 * 通話履歴生成タスクのパラメータを作成する
	 *
	 * @return
	 */
	List<Params> createParamsList() {
		Date minDate = config.historyMinDate;
		Date maxDate = config.historyMaxDate;

		statistics = new Statistics(minDate, maxDate);

		List<Duration> durationList = ContractInfoReader.initDurationList(config);
		if (!isValidDurationList(durationList, minDate, maxDate)) {
			throw new RuntimeException("Invalid duration list.");
		}

		long numberOfTasks = (config.numberOfHistoryRecords + config.maxNumberOfLinesHistoryCsv - 1)
				/ config.maxNumberOfLinesHistoryCsv;
		if (numberOfTasks > Integer.MAX_VALUE) {
			throw new RuntimeException("Too many numberOfTasks: " + numberOfTasks);
		}

		List<Params> paramsList = createTaskParams(minDate.getTime(), maxDate.getTime(),
				config.numberOfHistoryRecords, (int)numberOfTasks);
		return paramsList;
	}



	// 終了済みのタスク数
	private int numberOfEndTasks = 0;


	/**
	 * 通話履歴を生成するタスクを実行し、生成された通話履歴を書き出す
	 *
	 * @throws IOException
	 * @throws SQLException
	 */
	public void generateHistory(List<Params> paramsList) throws IOException, SQLException {

		// 通話履歴を生成するタスクとスレッドの生成
		ExecutorService service = Executors.newFixedThreadPool(config.threadCount);

		int numberOfTasks = paramsList.size();
		Set<Future<Result>> futureSet = new HashSet<>(paramsList.size());
		numberOfEndTasks = 0;
		for (Params params : paramsList) {
			GenerateHistoryTask task = new GenerateHistoryTask(params);
			Future<Result> future = service.submit(task);
			futureSet.add(future);
			waitFor(numberOfTasks, futureSet);
		}
		LOG.info(String.format("%d tasks sumbitted.", numberOfTasks));

		service.shutdown();

		while(!futureSet.isEmpty()) {
			waitFor(numberOfTasks, futureSet);
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				// Noting to do
			}
		}
	}

	/**
	 * 終了したタスクを調べる
	 *
	 * @param numberOfTasks
	 * @param futureSet
	 */
	@SuppressFBWarnings("DM_EXIT")
	private void waitFor(int numberOfTasks, Set<Future<Result>> futureSet) {
		Iterator<Future<Result>> it = futureSet.iterator();
		while (it.hasNext()) {
			Future<Result> future = it.next();
			if (future.isDone()) {
				Result result;
				try {
					result = future.get();
				} catch (InterruptedException | ExecutionException e) {
					result = new Result(-1);
					result.success = false;
					result.e = e;
				}
				if (!result.success) {
					LOG.error("Task(id = {}) finished with error, aborting...", result.taskId, result.e);
					System.exit(1);
				}
				it.remove();
				LOG.info(String.format("%d/%d tasks finished.", ++numberOfEndTasks, numberOfTasks));
			}
		}
	}



	/**
	 * 通話履歴をインサートする
	 *
	 * @param conn
	 * @param histories
	 * @throws SQLException
	 */
	public static void insrtHistories(Connection conn, List<History> histories) throws SQLException {
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
				long start = duration.start;
				long end = duration.end == null ? Long.MAX_VALUE : duration.end;
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

	private static void execBatch(PreparedStatement ps) throws SQLException {
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
	 * 通話履歴を書き出す抽象クラス.
	 */
	public abstract class HistoryWriter {
		/**
		 * クラスを初期化する
		 * @throws IOException
		 */
		abstract void init() throws IOException;


		/**
		 * 1レコード分出力する
		 * @throws IOException
		 * @throws SQLException
		 */
		abstract void write(History h) throws IOException, SQLException;

		/**
		 * クリーンナップ処理
		 * @throws IOException
		 * @throws SQLException
		 */
		abstract void cleanup() throws IOException, SQLException;
	}

	/**
	 * CSV出力用クラス
	 *
	 */
	private class CsvHistoryWriter extends HistoryWriter {
		private BufferedWriter bw;
		private StringBuilder sb;
		private Path outputPath;

		public CsvHistoryWriter(Path outputPath) {
				this.outputPath = outputPath;
		}

		@Override
		void init() throws IOException {
			bw = Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8,
					StandardOpenOption.TRUNCATE_EXISTING,
					StandardOpenOption.CREATE,
					StandardOpenOption.WRITE);
			sb = new StringBuilder();
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

	/**
	 * DB出力用クラス
	 *
	 */
	private class DbHistoryWriter extends HistoryWriter {
		List<History> histories = null;
		Connection conn = null;

		@Override
		void init() throws IOException {
			histories = new ArrayList<History>(SQL_BATCH_EXEC_SIZE);
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
			if (statisticsOnly) {
				statistics.addHistoy(h);
			} else {
				histories.add(h);
				if (histories.size() >= SQL_BATCH_EXEC_SIZE) {
					insrtHistories(conn, histories);
					histories.clear();
				}
			}
		}
	}

	/**
	 * オンラインアプリケーション用のダミーのHistoryWriter
	 *
	 */
	private class DummyHistoryWriter  extends HistoryWriter {

		@Override
		void init() throws IOException {
		}

		@Override
		void write(History h) throws IOException, SQLException {
		}

		@Override
		void cleanup() throws IOException, SQLException {
		}

	}


	/**
	 * 通話履歴のCSVファイルのパスを取得する
	 *
	 * @param dir CSVファイルのディレクトリ
	 * @return
	 */
	public static List<Path> getHistortyFilePaths(Path dir) {
		return null;
	}


	/**
	 * 契約のCSVSファイルのパスを取得する
	 *
	 * @param dir CSVファイルのディレクトリ
	 * @return
	 */
	public static Path getContractsFilePath(Path dir) {
		return null;
	}


	/**
	 * n番目の通話履歴のCSVファイルのパスを取得する
	 *
	 * @param n
	 * @param dir CSVファイルのディレクトリ
	 * @return
	 */
	public static Path getHistortyFilePath(Path dir, int n) {
		return null;
	}
}
