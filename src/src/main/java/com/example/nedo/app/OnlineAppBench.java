package com.example.nedo.app;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.app.billing.PhoneBill;
import com.example.nedo.db.DBUtils;
import com.example.nedo.testdata.CreateTestData;

/**
 * 以下の条件を変えて、バッチの処理時間がどう変化するのかを測定する
 * <ul>
 *   <li> オンラインアプリケーションを動かさない場合
 *   <li> 各オンラインアプリケーションを単独で動かした場合
 *   <li> すべてのオンラインアプリケーションを動かした場合
 * </ul>
 * 上記以外の値はConfigで指定された値を使用する
 *
 */
public class OnlineAppBench implements ExecutableCommand {
    private static final Logger LOG = LoggerFactory.getLogger(OnlineAppBench.class);

	public static void main(String[] args) throws Exception {
		OnlineAppBench threadBench = new OnlineAppBench();
		Config config = Config.getConfig(args);
//		Config config = Config.getConfig();
		threadBench.execute(config);
	}


	@Override
	public void execute(Config config) throws Exception {
		int historyInsertTransactionPerMin = config.historyInsertTransactionPerMin;
		int historyUpdateRecordsPerMin = config.historyUpdateRecordsPerMin;
		int masterInsertReccrdsPerMin = config.masterInsertReccrdsPerMin;
		int masterUpdateRecordsPerMin = config.masterUpdateRecordsPerMin;
		long elapsedTime;

		// オンラインアプリケーションを動かさない場合
		config.historyInsertTransactionPerMin = 0;
		config.historyUpdateRecordsPerMin = 0;
		config.masterInsertReccrdsPerMin = 0;
		config.masterUpdateRecordsPerMin = 0;
		elapsedTime = execBatch(config);
		LOG.info("No online application, elapsed time = {} ms", elapsedTime);

		// 各オンラインアプリケーションを単独で動かした場合
		config.historyInsertTransactionPerMin = historyInsertTransactionPerMin;
		config.historyUpdateRecordsPerMin = 0;
		config.masterInsertReccrdsPerMin = 0;
		config.masterUpdateRecordsPerMin = 0;
		elapsedTime = execBatch(config);
		LOG.info("History insert online application, elapsed time = {} ms", elapsedTime);

		config.historyInsertTransactionPerMin = 0;
		config.historyUpdateRecordsPerMin = historyUpdateRecordsPerMin;
		config.masterInsertReccrdsPerMin = 0;
		config.masterUpdateRecordsPerMin = 0;
		elapsedTime = execBatch(config);
		LOG.info("History update online application, elapsed time = {} ms", elapsedTime);

		config.historyInsertTransactionPerMin = 0;
		config.historyUpdateRecordsPerMin = 0;
		config.masterInsertReccrdsPerMin = masterInsertReccrdsPerMin;
		config.masterUpdateRecordsPerMin = 0;
		elapsedTime = execBatch(config);
		LOG.info("Master insert online application, elapsed time = {} ms", elapsedTime);

		config.historyInsertTransactionPerMin = 0;
		config.historyUpdateRecordsPerMin = 0;
		config.masterInsertReccrdsPerMin = 0;
		config.masterUpdateRecordsPerMin = masterUpdateRecordsPerMin;
		elapsedTime = execBatch(config);
		LOG.info("Master update online application, elapsed time = {} ms", elapsedTime);

		// すべてのオンラインアプリケーションを動かした場合
		config.historyInsertTransactionPerMin = historyInsertTransactionPerMin;
		config.historyUpdateRecordsPerMin = historyUpdateRecordsPerMin;
		config.masterInsertReccrdsPerMin = historyUpdateRecordsPerMin;
		config.masterUpdateRecordsPerMin = masterUpdateRecordsPerMin;
		elapsedTime = execBatch(config);
		LOG.info("All online application, elapsed time = {} ms", elapsedTime);
	}


	/**
	 * 指定のconfigでバッチを実行し、処理時間を返す
	 *
	 * @param config
	 * @return
	 * @throws Exception
	 */
	private long execBatch(Config config) throws Exception {
		PhoneBill phoneBill = new PhoneBill();
		new CreateTable().execute(config);
		new CreateTestData().execute(config);
		beforeExec(config);
		phoneBill.execute(config);
		afterExec(config);
		return phoneBill.getElapsedTime();
	}


	private void afterExec(Config config) throws SQLException {
		boolean isOracle = config.url.toLowerCase().contains("oracle");
		try (Connection conn = DBUtils.getConnection(config)) {
			conn.setAutoCommit(true);
			Statement stmt = conn.createStatement();
			int historyUpdated = count(stmt,
					"select caller_phone_number, recipient_phone_number, payment_categorty, start_time, time_secs, df from history_back",
					"select caller_phone_number, recipient_phone_number, payment_categorty, start_time, time_secs, df from history",
					isOracle);
			int historyInserted = count(stmt, "history") - count(stmt, "history_back");
			int masterUpdated = count(stmt,
					"select * from contracts_back",
					"select * from contracts",
					isOracle);
			int masterInserted = count(stmt, "contracts") - count(stmt, "contracts_back");
			LOG.info("history updated = " + historyUpdated);
			LOG.info("history inserted = " + historyInserted);
			LOG.info("master updated = " + masterUpdated);
			LOG.info("master inserted = " + masterInserted);
		}

	}


	int count(Statement stmt , String sql1, String sql2, boolean isOracle) throws SQLException {
		String sql;
		if (isOracle) {
			sql = "select count(*) from(" + sql1 + " minus " + sql2 + ")";
		} else {
			sql = "select count(*) from(" + sql1 + " except " + sql2 + ") as subq";
		}
		ResultSet rs = stmt.executeQuery(sql);
		if (rs.next()) {
			return rs.getInt(1);
		}
		throw new RuntimeException("No recoreds selected.");
	}

	int count(Statement stmt, String table) throws SQLException {
		String sql = "select count(*) from " + table;
		ResultSet rs = stmt.executeQuery(sql);
		if (rs.next()) {
			return rs.getInt(1);
		}
		throw new RuntimeException("No recoreds selected.");
	}

	private void beforeExec(Config config) throws SQLException {
		try (Connection conn = DBUtils.getConnection(config)) {
			conn.setAutoCommit(true);
			Statement stmt = conn.createStatement();
			dropTable(stmt, "history_back");
			dropTable(stmt, "contracts_back");
			stmt.execute("create table history_back as select * from history");
			stmt.execute("create table contracts_back as select * from contracts");
		}
	}

	void dropTable(Statement stmt, String table) throws SQLException {
		try {
			stmt.execute("drop table "+ table);
		} catch (SQLException e) {
			// drop tableでエラーが起きても無視する
		}
	}


}
