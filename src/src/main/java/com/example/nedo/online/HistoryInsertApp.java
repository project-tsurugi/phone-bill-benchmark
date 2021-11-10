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
import com.example.nedo.testdata.ContractBlockInfoAccessor;
import com.example.nedo.testdata.GenerateHistoryTask;
import com.example.nedo.testdata.HistoryKey;
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
	private long baseTime;
	private int duration;
	private List<History> histories = new ArrayList<>();
	private Random random;

	/**
	 * // 同一のPKのレコードを生成しないためにPK値を記録するためのセット
	 */
	private Set<HistoryKey> keySet = new HashSet<HistoryKey>();

	/**
	 * コンストラクタ
	 *
	 * @param contractInfoReader
	 * @param config
	 * @param seed
	 * @param baseTime
	 * @param duration
	 * @throws SQLException
	 * @throws IOException
	 */
	private HistoryInsertApp(ContractBlockInfoAccessor accessor, Config config, Random random, long baseTime,
			int duration) throws SQLException, IOException {
		super(config.historyInsertTransactionPerMin, config, random);
		this.historyInsertRecordsPerTransaction = config.historyInsertRecordsPerTransaction;
		this.baseTime = baseTime;
		this.duration = duration;
		this.random = random;
		TestDataGenerator testDataGenerator = new TestDataGenerator(config, random, accessor);
		generateHistoryTask = testDataGenerator.getGenerateHistoryTaskForOnlineApp();
	}

	/**
	 * 指定した数だけ、HistoryInsertAppのインスタンスを作成し、リストで返す.
	 * <br>
	 * 複数のHistoryInsertAppを同時に動かしても、キーの重複が起きないように、baseTimeとdurationの値を調整する。
	 *
	 * @param contractInfoReader
	 * @param config
	 * @param seed
	 * @param num
	 * @return
	 * @throws IOException
	 * @throws SQLException
	 */
	public static List<AbstractOnlineApp> createHistoryInsertApps(Config config,
			Random random, ContractBlockInfoAccessor accessor, int num) throws SQLException, IOException {
		List<AbstractOnlineApp> list = new ArrayList<>();
		if (num > 0) {
			int duration = CREATE_SCHEDULE_INTERVAL_MILLS / num;
			long baseTime = getBaseTime(config);
			for (int i = 0; i < num; i++) {
				if (i != 0) {
					random = new Random(random.nextInt());
				}
				AbstractOnlineApp app = new HistoryInsertApp(accessor, config, random, baseTime, duration);
				app.setName(i);
				baseTime += duration;
				list.add(app);
			}
		}
		return list;
	}


	@Override
	protected void atScheduleListCreated(List<Long> scheduleList) {
		// スケジュールに合わせてbaseTimeをシフトする
		baseTime = getBaseTime() + CREATE_SCHEDULE_INTERVAL_MILLS;
		// baseTimeのシフトにより、これ以前のキーとキーが重複することはないので、keySetをクリアする
		keySet.clear();
		// スケジュール作成時に、契約マスタのブロック情報をアップデートする
		generateHistoryTask.reloadActiveBlockNumberList();
	}

	/**
	 * baseTimeをセットする。履歴データの通話開始時刻は初期データの通話開始時刻の最後の日の翌日0時にする。
	 * ただし、既にhistoryInsertAppによるデータが存在する場合は、津和解し時刻の最大値を指定する。
	 *
	 * @param config
	 * @return
	 * @throws SQLException
	 */
	static long getBaseTime(Config config) throws SQLException {
		return Math.max(getMaxStartTime(config), DBUtils.nextDate(config.historyMaxDate).getTime());
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
			HistoryKey key;
			do {
				long startTime = getBaseTime() + random.nextInt(duration);
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

	/**
	 * baseTimeを返す(UT用)
	 *
	 * @return
	 */
	long getBaseTime() {
		return baseTime;
	}
}
