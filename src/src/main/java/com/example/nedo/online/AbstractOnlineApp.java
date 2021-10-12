package com.example.nedo.online;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.app.Config;
import com.example.nedo.db.DBUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public abstract class AbstractOnlineApp implements Runnable{
	/**
	 * スケジュールを生成するインターバル(ミリ秒)
	 */
	protected static final int CREATE_SCHEDULE_INTERVAL_MILLS  = 60 * 1000;

    private static final Logger LOG = LoggerFactory.getLogger(AbstractOnlineApp.class);
	private Connection conn;


	/**
	 * 実行回数
	 */
	private int execCount = 0;

	/**
	 * リトライ回数
	 */
	private int retryCount = 0;


	/**
	 * 終了リクエストの有無を表すフラグ
	 */
	private volatile boolean terminationRequest = false;

	/*
	 * 1分間に実行するトランザクション数、負数の場合は連続で実行する
	 */
	private int txPerMin;

	/**
	 * スケジュール作成時に使用する乱数発生器
	 */
	private Random random;

	/**
	 * スレッドの開始時刻
	 */
	private long startTime;

	/**
	 * 処理を実行する時刻を格納したセット
	 */
	private List<Long> scheduleList = new LinkedList<Long>();


	/**
	 * タスク名(=スレッド名)
	 */
	private String name;


	public AbstractOnlineApp(int txPerMin, Config config) throws SQLException {
		this.txPerMin = txPerMin;
		this.random = new Random(config.randomSeed);
		conn = DBUtils.getConnection(config);
		setName(0);
	}

	/**
	 * 指定の番号をもつタスク名をセットする
	 *
	 * @param num
	 */
	public void setName(int num) {
		name = this.getClass().getName().replaceAll(".*\\.", "") + "-" + String.format("%03d", num);
	}

	/**
	 * オンラインアプリの処理.
	 *
	 * createData()でデータを生成し、updateDatabase()で生成したデータを
	 * DBに反映する。updateDatabase()でリトライ可能なSQLExceptionが発生した場合、
	 * updateDatabase()をリトライする。
	 *
	 *
	 * @throws SQLException
	 */
	final void exec() throws SQLException {
		createData();
		for (;;) {
			try {
				updateDatabase();
				conn.commit();
				execCount++;
				break;
			} catch (SQLException e) {
				if (DBUtils.isRetriableSQLException(e)) {
					LOG.debug("{} caught a retriable exception, ErrorCode = {}, SQLStatus = {}.",
							this.getClass().getName(), e.getErrorCode(), e.getSQLState(), e);
					conn.rollback();
					retryCount++;
				} else {
					throw e;
				}
			}
		}
	}

	/**
	 * DBに入れるデータを生成する
	 * @throws SQLException
	 */
	protected abstract void createData() throws SQLException;

	/**
	 * createDataで生成したデータをDBに反映する
	 * @throws SQLException
	 */
	protected abstract void updateDatabase() throws SQLException;


	@Override
	@SuppressFBWarnings("DM_EXIT")
	public void run() {
		try {
			Thread.currentThread().setName(name);
			if (txPerMin == 0) {
				// txPerMinが0の場合は何もしない
				return;
			}
			LOG.info("{} started.", name);
			startTime = System.currentTimeMillis();
			scheduleList.add(startTime);
			while (!terminationRequest) {
				schedule();
			}
			LOG.info("{} terminated.", name);
		} catch (Exception e) {
			try {
				if (conn != null && !conn.isClosed()) {
					conn.rollback();
				}
			} catch (SQLException e1) {
				throw new RuntimeException(e1);
			}
			LOG.error("Aborting by exception", e);
			System.exit(1);
		} finally {
			try {
				if (conn != null && !conn.isClosed()) {
					conn.close();
				}
			} catch (Exception e) {
				LOG.error("{} cleanup failure due to exception.", name, e);
			}
		}
	}


	/**
	 * スケジュールに従いexec()を呼び出す
	 * @throws SQLException
	 */
	private void schedule() throws SQLException {
		Long schedule = scheduleList.get(0);
		if (System.currentTimeMillis() < schedule ) {
			if (txPerMin > 0) {
				// 処理の開始時刻になっていなければ、10ミリ秒スリープしてリターンする
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					// Nothing to do;
				}
				return;
			} else {
				// 連続実行が指定されているケース
				exec();
				return;
			}
		}
		// スケジュール時刻になったとき
		scheduleList.remove(0);
		if (scheduleList.isEmpty()) {
			// スケジュールリストの最後のエントリはスケジュールを作成する時刻
			creatScheduleList(schedule);
		} else {
			exec();
		}
	}


	/**
	 * スケジュールを作成する
	 *
	 * @param base スケジュール生成のスケジュール(時刻)
	 */
	private void creatScheduleList(long base) {
		long now = System.currentTimeMillis();
		if (base + CREATE_SCHEDULE_INTERVAL_MILLS < now) {
			// スケジュール生成の呼び出しが、予定よりCREATE_SCHEDULE_INTERVAL_MILLSより遅れた場合は、
			// 警告のログを出力し、スケジュールのベースとなる時刻を進める。
			LOG.warn("Detected a large delay in the schedule and reset the base time(base = {}, now = {}).",
					new Timestamp(base), new Timestamp(now));
			base = System.currentTimeMillis();
		}
		for (int i = 0; i < txPerMin; i++) {
			long schedule = base + random.nextInt(CREATE_SCHEDULE_INTERVAL_MILLS);
			scheduleList.add(schedule);
		}
		Collections.sort(scheduleList);
		// 次にスケジュールを作成する時刻
		scheduleList.add(base + CREATE_SCHEDULE_INTERVAL_MILLS);
		atScheduleListCreated(scheduleList);
	}

	/**
	 * スケジュール作成後に呼び出されるコールバック関数
	 *
	 * @param scheduleList
	 */
	protected void atScheduleListCreated(List<Long> scheduleList) {
		// デフォルト実装ではなにもしない
	}


	/**
	 * このオンラインアプリケーションをアボートする
	 */
	public void terminate() {
		terminationRequest = true;
	}


	/**
	 * @return DBコネクション
	 */
	protected Connection getConnection() {
		return conn;
	}

	/**
	 * @return execCount
	 */
	public int getExecCount() {
		return execCount;
	}

	/**
	 * @return retryCount
	 */
	public int getRetryCount() {
		return retryCount;
	}
}
