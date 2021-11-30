package com.example.nedo.online;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

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
	private AtomicInteger execCount = new AtomicInteger(0);

	/**
	 * リトライ回数
	 */
	private AtomicInteger retryCount = new AtomicInteger(0);

	/**
	 * 終了リクエストの有無を表すフラグ
	 */
	private volatile boolean terminationRequested = false;

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
	 * データベースアクセスをスキップすることを示すフラグ
	 */
	protected final boolean skipDatabaseAccess;


	/**
	 * タスク名(=スレッド名)
	 */
	private String name;

	/**
	 * タスク名のベース
	 */
	private String baseName;


	public AbstractOnlineApp(int txPerMin, Config config, Random random) throws SQLException {
		this.txPerMin = txPerMin;
		this.random = random;
		conn = DBUtils.getConnection(config);
		skipDatabaseAccess = config.skipDatabaseAccess;
		setName(0);
	}

	/**
	 * 指定の番号をもつタスク名をセットする
	 *
	 * @param num
	 */
	public void setName(int num) {
		baseName = this.getClass().getName().replaceAll(".*\\.", "");
		name = baseName + "-" + String.format("%03d", num);
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
	 * @throws IOException
	 */
	final void exec() throws SQLException, IOException {
		createData();
		for (;;) {
			try {
				if (!skipDatabaseAccess) {
					updateDatabase();
					conn.commit();
				}
				execCount.incrementAndGet();
				break;
			} catch (SQLException e) {
				if (DBUtils.isRetriableSQLException(e)) {
					LOG.debug("{} caught a retriable exception, ErrorCode = {}, SQLStatus = {}.",
							this.getClass().getName(), e.getErrorCode(), e.getSQLState(), e);
					conn.rollback();
					retryCount.incrementAndGet();
				} else {
					throw e;
				}
			}
		}
	}

	/**
	 * DBに入れるデータを生成する
	 * @throws SQLException
	 * @throws IOException
	 */
	protected abstract void createData() throws SQLException, IOException;

	/**
	 * createDataで生成したデータをDBに反映する
	 * @throws SQLException
	 * @throws IOException
	 */
	protected abstract void updateDatabase() throws SQLException, IOException;


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
			while (!terminationRequested) {
				schedule();
			}
			LOG.info("{} terminated.", name);
		} catch (RuntimeException | SQLException | IOException e) {
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
	 * @throws IOException
	 */
	private void schedule() throws SQLException, IOException {
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
	 * @throws IOException
	 */
	private void creatScheduleList(long base) throws IOException {
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
	 * @throws IOException
	 */
	protected void atScheduleListCreated(List<Long> scheduleList) throws IOException {
		// デフォルト実装ではなにもしない
	}


	/**
	 * このオンラインアプリケーションをアボートする
	 */
	public void terminate() {
		terminationRequested = true;
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
		return execCount.intValue();
	}

	/**
	 * @return retryCount
	 */
	public int getRetryCount() {
		return retryCount.intValue();
	}

	/**
	 * @return name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @return baseName
	 */
	public String getBaseName() {
		return baseName;
	}
}
