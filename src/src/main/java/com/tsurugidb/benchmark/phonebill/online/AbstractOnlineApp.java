package com.tsurugidb.benchmark.phonebill.online;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager.CounterName;
import com.tsurugidb.benchmark.phonebill.db.TxLabel;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.TxOption.Table;
import com.tsurugidb.benchmark.phonebill.db.dao.ContractDao;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public abstract class AbstractOnlineApp implements Runnable{
	/**
	 * スケジュールを生成するインターバル(ミリ秒)
	 */
	protected static final int CREATE_SCHEDULE_INTERVAL_MILLS  = 60 * 1000;

    private static final Logger LOG = LoggerFactory.getLogger(AbstractOnlineApp.class);

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
	private AtomicBoolean terminationRequested = new AtomicBoolean(false);

	/*
	 * 1分間に実行する回数、負数の場合は連続で実行する
	 */
	private int execPerMin;

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


	// Config
	private Config config;


	/**
	 * OCCのTxOption
	 */
	private TxOption occ;

	/**
	 * LTXのTxOption
	 */
	private TxOption ltx;



	public AbstractOnlineApp(int execPerMin, Config config, Random random) {
		this.execPerMin = execPerMin;
		this.random = random;
		this.config = config;
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
	 * DBに反映する。
	 * @param manager
	 */
	final void exec(PhoneBillDbManager manager) {
		if (occ == null) {
			occ = TxOption.ofOCC(0, getTxLabel());
		}
		boolean occSuccess =	exec(manager, occ, 3, true);
		if (occSuccess) {
			return;
		}
		if (ltx == null) {
			ltx = TxOption.ofLTX(0, getTxLabel(), getWritePreserveTable());
		}
		boolean ltxSuccess = 	exec(manager, ltx, 3, false);
		if (ltxSuccess) {
			return;
		}
		manager.countup(ltx, CounterName.ABANDONED_RETRY);
		return;
	}

	/**
	 * 指定のDbManager, TxOption, リトライ回数で実行する
	 *
	 * @param manager
	 * @param maxTry
	 * @param txOption
	 * @return リトライしても成功しない場合false、成功した場合true、終了要求により終了する場合true
	 */
	private boolean exec(PhoneBillDbManager manager, TxOption txOption, int maxTry, boolean isOcc) {
		HistoryDao historyDao = manager.getHistoryDao();
		ContractDao contractDao = manager.getContractDao();
		for (int i = 0; i < maxTry; i++) {
			if (terminationRequested.get()) {
				return true;
			}
			try {
				manager.execute(txOption, () -> {
					manager.countup(txOption, isOcc ? CounterName.OCC_TRY : CounterName.LTX_TRY);
					createData(contractDao, historyDao);
					if (!skipDatabaseAccess) {
						updateDatabase(contractDao, historyDao);
					}
				});
				manager.countup(txOption, isOcc ? CounterName.OCC_SUCC : CounterName.LTX_SUCC);
				return true;
			} catch (RuntimeException e) {
				manager.countup(txOption, isOcc ? CounterName.OCC_ABORT : CounterName.LTX_ABORT);
				PhoneBillDbManager.addRetringExceptions(e);
				LOG.debug("Tx aborted by caught an exception ", e.getMessage());
				continue;
			}
		}
		return false;
	}

	/**
	 * DBに入れるデータを生成する
	 * @param historyDao
	 * @param contractDao
	 * @throws IOException
	 */
	protected abstract void createData(ContractDao contractDao, HistoryDao historyDao);

	/**
	 * createDataで生成したデータをDBに反映する
	 * @param historyDao
	 * @param contractDao
	 * @throws IOException
	 */
	protected abstract void updateDatabase(ContractDao contractDao, HistoryDao historyDao);


	@Override
	@SuppressFBWarnings("DM_EXIT")
	public void run() {
		try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)){
			Thread.currentThread().setName(name);
			if (execPerMin == 0) {
				// txPerMinが0の場合は何もしない
				return;
			}
			LOG.info("{} started.", name);
			startTime = System.currentTimeMillis();
			scheduleList.add(startTime);
			while (!terminationRequested.get()) {
				schedule(manager);
			}
			LOG.info("{} terminated.", name);
		} catch (RuntimeException | IOException e) {
			LOG.error("Aborting by exception", e);
			System.exit(1);
		}
	}

	/**
	 * スケジュールに従いexec()を呼び出す
	 * @param manager
	 * @throws IOException
	 */
	private void schedule(PhoneBillDbManager manager) throws IOException {
		Long schedule = scheduleList.get(0);
		if (System.currentTimeMillis() < schedule ) {
			if (execPerMin > 0) {
				// 処理の開始時刻になっていなければ、10ミリ秒スリープしてリターンする
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					// Nothing to do;
				}
				return;
			} else {
				// 連続実行が指定されているケース
				exec(manager);
				return;
			}
		}
		// スケジュール時刻になったとき
		scheduleList.remove(0);
		if (scheduleList.isEmpty()) {
			// スケジュールリストの最後のエントリはスケジュールを作成する時刻
			creatScheduleList(schedule);
		} else {
			exec(manager);
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
		for (int i = 0; i < execPerMin; i++) {
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
		terminationRequested.set(true);
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

	/**
	 * オンラインアプリが使用するTxLabelを返す
	 *
	 * @return
	 */
	public abstract TxLabel getTxLabel();


	/**
	 * オンラインアプリが書き込むテーブルを返す
	 *
	 * @return
	 */
	public abstract Table getWritePreserveTable();
}

