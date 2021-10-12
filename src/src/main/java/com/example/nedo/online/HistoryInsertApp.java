package com.example.nedo.online;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.app.Config;
import com.example.nedo.db.DBUtils;
import com.example.nedo.db.History;
import com.example.nedo.testdata.GenerateHistoryTask;
import com.example.nedo.testdata.GenerateHistoryTask.Key;
import com.example.nedo.testdata.TestDataGenerator;

/**
 * 通話履歴を追加するオンラインアプリケーション.
 * <br>
 * 話開始時刻が、baseTime ～ baseTime + durationの通話履歴を生成する。atScheduleListCreated()が
 * 呼び出されるたびに、baseTimeをCREATE_SCHEDULE_INTERVAL_MILLSだけシフトする。
 *
 */
public class HistoryInsertApp extends AbstractOnlineApp {
    private static final Logger LOG = LoggerFactory.getLogger(HistoryInsertApp.class);
	private int historyInsertRecordsPerTransaction;
	private GenerateHistoryTask generateHistoryTask;
	long baseTime; //
	int duration;
	private Random random;
	List<History> histories = new ArrayList<>();

	/**
	 * // 同一のPKのレコードを生成しないためにPK値を記録するためのセット
	 */
	private Set<Key> keySet;

	/**
	 * コンストラクタ
	 *
	 * @param contractHolder
	 * @param config
	 * @param seed
	 * @param baseTime
	 * @param duration
	 * @throws SQLException
	 * @throws IOException
	 */
	private HistoryInsertApp(ContractHolder contractHolder, Config config, int seed, long baseTime, int duration) throws SQLException, IOException {
		super(config.historyInsertTransactionPerMin, config);
		this.random = new Random(seed);
		historyInsertRecordsPerTransaction = config.historyInsertRecordsPerTransaction;
		this.baseTime = baseTime;
		this.duration = duration;
		keySet = new HashSet<Key>(); // 同一の時刻のレコードを生成しないために時刻を記録するためのセット
		TestDataGenerator testDataGenerator = new TestDataGenerator(config);
		generateHistoryTask = testDataGenerator.getGenerateHistoryTaskForOnlineApp();
	}


	/**
	 * 指定した数だけ、HistoryInsertAppのインスタンスを作成し、リストで返す.
	 * <br>
	 * 複数のHistoryInsertAppを同時に動かしても、キーの重複が起きないように、baseTimeとdurationの値を調整する。
	 *
	 * @param contractHolder
	 * @param config
	 * @param seed
	 * @param num
	 * @return
	 * @throws IOException
	 * @throws SQLException
	 */
	public static List<AbstractOnlineApp> createHistoryInsertApps(ContractHolder contractHolder, Config config,
			int seed, int num) throws SQLException, IOException {
		List<AbstractOnlineApp> list = new ArrayList<>();
		if (num > 0) {
			int duration = CREATE_SCHEDULE_INTERVAL_MILLS / num;
			long baseTime = getMaxStartTime(config);
			Random random = new Random(seed);
			for (int i = 0; i < num; i++) {
				AbstractOnlineApp app = new HistoryInsertApp(contractHolder, config, random.nextInt(), baseTime,
						duration);
				app.setName(i);
				baseTime += duration;
				list.add(app);
			}
		}
		return list;
	}




	@Override
	protected void atScheduleListCreated(List<Long> scheduleList) {
		baseTime += CREATE_SCHEDULE_INTERVAL_MILLS;
		keySet.clear();
	}

	static long getMaxStartTime(Config config) throws SQLException {
		try (Connection conn = DBUtils.getConnection(config);
				Statement stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery("select max(start_time) from history");) {
			if (rs.next()) {
				Timestamp ts = rs.getTimestamp(1);
				conn.commit();
				return ts == null ? System.currentTimeMillis() : ts.getTime();
			}
		}
		throw new IllegalStateException();
	}

	@Override
	protected void createData() {
		histories.clear();
		for (int i = 0; i < historyInsertRecordsPerTransaction; i++) {
			Key key;
			do {
				long startTime = baseTime + random.nextInt(duration);
				key = generateHistoryTask.createkey(startTime);
			} while (keySet.contains(key));
			keySet.add(key);
			histories.add(generateHistoryTask.createHistoryRecord(key));
		}
	}

	@Override
	protected void updateDatabase() throws SQLException {
		TestDataGenerator.insrtHistories(getConnection(), histories);
		if (LOG.isDebugEnabled()) {
			LOG.debug("ONLINE APP: Insert {} records to history.", historyInsertRecordsPerTransaction);
		}
	}
}
