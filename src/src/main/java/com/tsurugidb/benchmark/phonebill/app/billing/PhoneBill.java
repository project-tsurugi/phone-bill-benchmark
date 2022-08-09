package com.tsurugidb.benchmark.phonebill.app.billing;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.ExecutableCommand;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.doma2.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.jdbc.DBUtils;
import com.tsurugidb.benchmark.phonebill.db.jdbc.Duration;
import com.tsurugidb.benchmark.phonebill.online.AbstractOnlineApp;
import com.tsurugidb.benchmark.phonebill.online.HistoryInsertApp;
import com.tsurugidb.benchmark.phonebill.online.HistoryUpdateApp;
import com.tsurugidb.benchmark.phonebill.online.MasterInsertApp;
import com.tsurugidb.benchmark.phonebill.online.MasterUpdateApp;
import com.tsurugidb.benchmark.phonebill.testdata.ActiveBlockNumberHolder;
import com.tsurugidb.benchmark.phonebill.testdata.ContractBlockInfoAccessor;
import com.tsurugidb.benchmark.phonebill.testdata.DbContractBlockInfoInitializer;
import com.tsurugidb.benchmark.phonebill.testdata.SingleProcessContractBlockManager;

public class PhoneBill extends ExecutableCommand {
    private static final Logger LOG = LoggerFactory.getLogger(PhoneBill.class);

    private PhoneBillDbManager manager;
    private long elapsedTime = 0; // バッチの処理時間
	private CalculationTargetQueue queue;
	private String finalMessage;
	private AtomicBoolean abortRequested = new AtomicBoolean(false);

	Config config;

	public static void main(String[] args) throws Exception {
		Config config = Config.setConfigForAppConfig(false);
		PhoneBill phoneBill = new PhoneBill();
		phoneBill.execute(config);
	}

	@Override
	public void execute(Config config) throws Exception {
		this.config = config;
		this.manager = config.getDbManager();
		DbContractBlockInfoInitializer initializer = new DbContractBlockInfoInitializer(config);
		ContractBlockInfoAccessor accessor = new SingleProcessContractBlockManager(initializer);
		List<AbstractOnlineApp> list = createOnlineApps(config, accessor);
		final ExecutorService service = list.isEmpty() ? null : Executors.newFixedThreadPool(list.size());
		try {
			// オンラインアプリを実行する
			list.parallelStream().forEach(task -> service.submit(task));

			// バッチを実行する
			Duration d = toDuration(config.targetMonth);
			doCalc(d.getStatDate(), d.getEndDate());
		} finally {
			// オンラインアプリを終了する
			list.stream().forEach(task -> task.terminate());
			if (service != null) {
				service.shutdown();
				service.awaitTermination(5, TimeUnit.MINUTES);
			}
		}
	}


	/**
	 * Configに従ってオンラインアプリのインスタンスを生成する
	 *
	 * @return オンラインアプリのインスタンスのリスト
	 * @throws SQLException
	 * @throws IOException
	 */
	public static List<AbstractOnlineApp> createOnlineApps(Config config, ContractBlockInfoAccessor accessor)
			throws SQLException, IOException {
		PhoneBillDbManager manager = config.getDbManager();
		Random random = new Random(config.randomSeed);
		ActiveBlockNumberHolder blockHolder = accessor.getActiveBlockInfo();
		if (blockHolder.getNumberOfActiveBlacks() < 1) {
			throw new IllegalStateException("Insufficient test data, create test data first.");
		}

		List<AbstractOnlineApp> list = new ArrayList<AbstractOnlineApp>();
		if (config.historyInsertThreadCount > 0 && config.historyInsertTransactionPerMin != 0) {
			list.addAll(HistoryInsertApp.createHistoryInsertApps(manager, config, new Random(random.nextInt()), accessor,
					config.historyInsertThreadCount));
		}
		if (config.historyUpdateThreadCount > 0 && config.historyUpdateRecordsPerMin != 0) {
			for (int i = 0; i < config.historyUpdateThreadCount; i++) {
				AbstractOnlineApp task = new HistoryUpdateApp(config, new Random(random.nextInt()), accessor);
				task.setName(i);
				list.add(task);
			}
		}
		if (config.masterInsertThreadCount > 0 && config.masterInsertReccrdsPerMin != 0) {
			for (int i = 0; i < config.masterInsertThreadCount; i++) {
				AbstractOnlineApp task = new MasterInsertApp(config, new Random(random.nextInt()), accessor);
				task.setName(i);
				list.add(task);
			}
		}
		if (config.masterUpdateThreadCount > 0 && config.masterUpdateRecordsPerMin != 0) {
			for (int i = 0; i < config.masterUpdateThreadCount; i++) {
				AbstractOnlineApp task = new MasterUpdateApp(config, new Random(random.nextInt()), accessor);
				task.setName(i);
				list.add(task);
			}
		}
		return list;
	}


	/**
	 * 指定の日付の一日から月の最終日までのDurationを作成する
	 *
	 * @param date
	 * @return
	 */
	static Duration toDuration(Date date) {
		LocalDate localDate = date.toLocalDate();
		Date start = Date.valueOf(localDate.withDayOfMonth(1));
		Date end = Date.valueOf(localDate.withDayOfMonth(1).plusMonths(1).minusDays(1));
		return new Duration(start, end);
	}


	/**
	 * 料金計算のメイン処理
	 *
	 * @param config
	 * @param start
	 * @param end
	 * @throws Exception
	 */
	void doCalc(Date start, Date end) throws SQLException {
		abortRequested.set(false);
		LOG.info("Phone bill batch started.");
		String batchExecId = UUID.randomUUID().toString();
		int threadCount = config.threadCount;
		boolean sharedConnection = config.sharedConnection;

		ExecutorService service = null;
		Set<Future<Exception>> futures = new HashSet<>(threadCount);
		List<Connection> connections = new ArrayList<Connection>(threadCount);
		initQueue(threadCount);

		long startTime = System.currentTimeMillis();
		try {
			Connection conn = DBUtils.getConnection(config);
			connections.add(conn);
			// 契約毎の計算を行うスレッドを生成する
			service = Executors.newFixedThreadPool(threadCount);
			for(int i =0; i < threadCount; i++) {
				if (sharedConnection) {
					futures.add(service.submit(new CalculationTask(queue, conn, config, batchExecId, abortRequested)));
				} else {
					Connection newConnection = DBUtils.getConnection(config);
					connections.add(newConnection);
					futures.add(service
							.submit(new CalculationTask(queue, newConnection, config, batchExecId, abortRequested)));
				}
			}

			// Billingテーブルの計算対象月のレコードを削除する
			deleteTargetManthRecords(conn, start);
			// 計算対象の契約を取りだし、キューに入れる
			String sql = "select phone_number, start_date, end_date, charge_rule"
					+ " from contracts where start_date <= ? and ( end_date is null or end_date >= ?)"
					+ " order by phone_number";
			try (PreparedStatement ps = conn.prepareStatement(sql)) {
				ps.setDate(1, end);
				ps.setDate(2, start);
				try (ResultSet contractResultSet = ps.executeQuery()) {
					while (contractResultSet.next()) {
						Contract contract = getContract(contractResultSet);
						LOG.debug(contract.toString());
						// TODO 契約内容に合致した、CallChargeCalculator, BillingCalculatorを生成するようにする。
						CallChargeCalculator callChargeCalculator = new SimpleCallChargeCalculator();
						BillingCalculator billingCalculator = new SimpleBillingCalculator();
						CalculationTarget target = new CalculationTarget(contract, billingCalculator,
								callChargeCalculator, start, end, false);
						queue.put(target);
						;
					}
				}
			}
		} finally {
			// EndOfTaskをキューに入れる
			queue.setInputClosed();
			if (service != null && !service.isTerminated()) {
				service.shutdown();
			}
			cleanup(futures, connections, abortRequested);
			queue.clear();
		}
		elapsedTime = System.currentTimeMillis() - startTime;
		String format = "Billings calculated in %,.3f sec ";
		finalMessage = String.format(format, elapsedTime / 1000d);
		LOG.info(finalMessage);
	}

	/**
	 * @param threadCount
	 */
	private synchronized void initQueue(int threadCount) {
		queue = new CalculationTargetQueue(threadCount);
	}

	public synchronized String getStatus() {
		return queue == null ? "Initializing": queue.getStatus();
	}



	/**
	 * @param conn
	 * @param futures
	 * @param connections
	 * @param abortRequested
	 * @throws SQLException
	 */
	private void cleanup(Set<Future<Exception>> futures, List<Connection> connections, AtomicBoolean abortRequested)
			throws SQLException {
		boolean needRollback = false;
		while (!futures.isEmpty()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				LOG.debug("InterruptedException caught and waiting service shutdown.", e);
			}
			Iterator<Future<Exception>> it = futures.iterator();
			while (it.hasNext()) {
				Exception e = null;
				{
					Future<Exception> future = it.next();
					if (future.isDone()) {
						it.remove();
						try {
							if (!future.isCancelled()) {
								e = future.get(0, TimeUnit.SECONDS);
							}
						} catch (InterruptedException e1) {
							continue;
						} catch (ExecutionException e1) {
							e = e1;
						} catch (TimeoutException e1) {
							continue;
						}
					}
				}
				if (e != null) {
					LOG.error("Exception cought", e);
					needRollback = true;
					abortRequested.set(true);
				}
			}
		}

		if (needRollback) {
			for (Connection c : connections) {
				if (c != null && !c.isClosed()) {
					c.rollback();
					c.close();
				}
			}
		} else {
			for (Connection c : connections) {
				if (c != null && !c.isClosed()) {
					c.commit();
					c.close();
				}
			}
		}
	}


	private void deleteTargetManthRecords(Connection conn, Date start) throws SQLException {
		String sql = "delete from billing where target_month = ?";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setDate(1, start);
			ps.executeUpdate();
		}
	}



	private Contract getContract(ResultSet rs) throws SQLException {
		Contract contract = new Contract();
		contract.phoneNumber = rs.getString(1);
		contract.startDate = rs.getDate(2);
		contract.endDate = rs.getDate(3);
		contract.rule = rs.getString(4);
		return contract;
	}

	/**
	 * バッチの処理時間を取得する
	 *
	 * @return elapsedTime
	 */
	public long getElapsedTime() {
		return elapsedTime;
	}

	/**
	 * 終了時のメッセージを返す
	 *
	 * @return
	 */
	public String getFinalMessage() {
		return finalMessage;
	}

	/**
	 * バッチを中断する
	 */
	public void abort() {
		abortRequested.set(true);
	}
}