package com.example.nedo.online;

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
import com.example.nedo.db.History;
import com.example.nedo.testdata.TestDataGenerator;

public class HistoryInsertApp extends AbstractOnlineApp {
    private static final Logger LOG = LoggerFactory.getLogger(HistoryInsertApp.class);
	private TestDataGenerator testDataGenerator;
	private int historyInsertRecordsPerTransaction;
	private long baseTime;
	private Random random;
	private Connection conn;
	List<History> histories = new ArrayList<>();

	/**
	 * // 同一の時刻のレコードを生成しないために時刻を記録するためのセット
	 */
	private Set<Key> startTimeSet;

	public HistoryInsertApp(ContractHolder contractHolder, Config config, int seed) throws SQLException {
		super(config.historyInsertTransactionPerMin, config);
		this.random = new Random(seed);
		historyInsertRecordsPerTransaction = config.historyInsertRecordsPerTransaction;
		testDataGenerator = new TestDataGenerator(config, seed, contractHolder);
		baseTime = getMaxStartTime(); // 通話履歴中の最新の通話開始時刻をベースに新規に作成する通話履歴の通話開始時刻を生成する
		conn = getConnection();
		startTimeSet = new HashSet<Key>(); // 同一の時刻のレコードを生成しないために時刻を記録するためのセット
	}

	@Override
	protected void atScheduleListCreated(List<Long> scheduleList) {
		baseTime += CREATE_SCHEDULE_INTERVAL_MILLS;
		startTimeSet.clear();
	}

	long getMaxStartTime() throws SQLException {
		Connection conn = getConnection();
		try (Statement stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery("select max(start_time) from history");) {
			if (rs.next()) {
				Timestamp ts = rs.getTimestamp(1);
				return ts == null ? System.currentTimeMillis() : ts.getTime();
			}
		}
		conn.commit();
		throw new IllegalStateException();
	}

	private static class Key {
		String phoneNumber;
		Long startTime;

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((phoneNumber == null) ? 0 : phoneNumber.hashCode());
			result = prime * result + ((startTime == null) ? 0 : startTime.hashCode());
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
			Key other = (Key) obj;
			if (phoneNumber == null) {
				if (other.phoneNumber != null)
					return false;
			} else if (!phoneNumber.equals(other.phoneNumber))
				return false;
			if (startTime == null) {
				if (other.startTime != null)
					return false;
			} else if (!startTime.equals(other.startTime))
				return false;
			return true;
		}
	}

	@Override
	protected void createData() {
		histories.clear();
		History  history;
		Key key;
		for (int i = 0; i < historyInsertRecordsPerTransaction; i++) {
			do {
				long startTime = baseTime + random.nextInt(CREATE_SCHEDULE_INTERVAL_MILLS);
				history = testDataGenerator.createHistoryRecord(startTime);
				key = new Key();
				key.phoneNumber = history.callerPhoneNumber;
				key.startTime = startTime;
			} while (startTimeSet.contains(key));
			startTimeSet.add(key);
			histories.add(history);
		}
	}

	@Override
	protected void updateDatabase() throws SQLException {
		testDataGenerator.insrtHistories(conn, histories);
		if (LOG.isDebugEnabled()) {
			LOG.debug("ONLINE APP: Insert {} records to history.", historyInsertRecordsPerTransaction);
		}
	}
}
