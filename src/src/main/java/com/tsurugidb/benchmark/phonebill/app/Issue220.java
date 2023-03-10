package com.tsurugidb.benchmark.phonebill.app;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.TxLabel;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.TxOption.Table;
import com.tsurugidb.benchmark.phonebill.db.dao.Ddl;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;
import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.benchmark.phonebill.testdata.PhoneNumberGenerator;

public class Issue220 extends ExecutableCommand {
    private static final Logger LOG = LoggerFactory.getLogger(Issue220.class);

	private static boolean LOGGING_DETAIL_TIME_INFO = true;
	private static final int[] THREAD_COUNTS = { 64, 16 };
	private static final TxOption OCC  =  TxOption.ofOCC(1, TxLabel.TEST);
	private static final TxOption LTX = TxOption.ofLTX(1, TxLabel.TEST, Table.HISTORY);

	private AtomicLong remaining;
	private AtomicInteger retryCounter;
	private static int recordsPerCommit;
	private List<Record> records = new ArrayList<>();
	private static String threadLabel = "";


	public static void main(String[] args) throws Exception {
		Config config = Config.getConfig();
		config.maxNumberOfLinesHistoryCsv=1;
		new Issue220().execute(config);
	}

	@Override
	public void execute(Config config) throws Exception {
		recordsPerCommit = config.maxNumberOfLinesHistoryCsv;
		LOG.info("Number of history records = {}, records per an commit = {}", config.numberOfHistoryRecords,
				recordsPerCommit);


		// テーブルの作成
		try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
			Ddl ddl = manager.getDdl();
			manager.execute(TxOption.of(), () -> {
				ddl.dropTables();
				ddl.createHistoryTable();
			});
		}



		// スレッド数、TxOptionを変えて複数回実行する
		for (int i = 0; i < 1; i++) {
			for (int threadCount : THREAD_COUNTS) {
				threadLabel = String.format("T%02d", threadCount);
//			execute(config, threadCount, () -> {
//				return new InsertTask(config, OCC, false);
//			}, "Insert, OCC");
//			execute(config, threadCount, () -> {
//				return new DeleteTask(config, OCC, false);
//			}, "Delete, OCC");
				execute(config, threadCount, () -> {
					return new InsertTask(config, LTX, false);
				}, "Insert, LTX");
				execute(config, threadCount, () -> {
					return new DeleteTask(config, LTX, false);
				}, "Delete, LTX");
			}
		}
	}

	public void execute(Config config, int threadCount, Supplier<Runnable> supplier, String description)
			throws InterruptedException, ExecutionException {


		Record record = new Record(description + ", T" + String.format("%02d", threadCount));
		record.start();
		records.add(record);
		remaining = new AtomicLong(config.numberOfHistoryRecords);
		retryCounter = new AtomicInteger(0);
		ExecutorService service = Executors.newFixedThreadPool(threadCount);
		List<Future<?>> futures = new ArrayList<>(threadCount);
		for (int i = 0; i < threadCount; i++) {
			Future<?> f = service.submit(supplier.get());
			futures.add(f);
		}
		TateyamaWatcher tateyamaWatcher = new TateyamaWatcher();
		Future<?> futureTateyamaWatchter = service.submit(tateyamaWatcher);


		service.shutdown();
		for(Future<?> f: futures) {
			f.get();
		}
		tateyamaWatcher.stop();
		futureTateyamaWatchter.get();
		record.end(config.numberOfHistoryRecords, tateyamaWatcher);
		try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
			HistoryDao dao = manager.getHistoryDao();
			int c = manager.execute(LTX, () -> {
				return  dao.count();
			});
			LOG.info("History table has {} records.", c);
		}
		// レポート出力
		System.out.println("Type, Option,Threads ,NumberOfRecords , Time(sec), RetryCount, VSZ, RSS");
		records.sort((r1, r2) -> r1.description.compareTo(r2.description));
		for (Record r: records) {
			r.print();
		}
	}



	private class InsertTask implements Runnable {
		private Config config;
		private TxOption txOption;
		private boolean skipDbAccess;

		public InsertTask(Config config, TxOption txOption, boolean skipDbAccess) {
			this.config = config;
			this.txOption = txOption;
			this.skipDbAccess = skipDbAccess;
		}

		@Override
		public void run() {
			long counter = 0;
			LOG.debug("Start insert task");

			PhoneNumberGenerator phoneNumberGenerator = new PhoneNumberGenerator(config);

			try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
				HistoryDao dao  = manager.getHistoryDao();
				Timer timer = new Timer("INSERT", txOption == LTX ? "LTX" : "OCC");
				for (;;) {
					long basePhoneNumber = remaining.getAndAdd(-recordsPerCommit);
					if (basePhoneNumber <= 0) {
						break;
					}
					int n = (int) (basePhoneNumber > recordsPerCommit ? recordsPerCommit : basePhoneNumber);
					History h = new History();
					h.setRecipientPhoneNumber(phoneNumberGenerator.to11DigtString(0));
					h.setPaymentCategorty("C");
					h.setStartTime(LocalDateTime.now());
					h.setTimeSecs(100);
					h.setCharge(0);
					h.setDf(0);
					List<History> list = new ArrayList<History>(n);
					for (int i = 0; i < n; i++) {
						h.setCallerPhoneNumber(phoneNumberGenerator.to11DigtString(basePhoneNumber - i));
						list.add(h.clone());
					}
					if (!skipDbAccess) {
						for (;;) {
							try {
								manager.execute(txOption, () -> {
									timer.setStartTx();
									dao.batchInsert(list);
									timer.setStartCommit();

								});
								timer.setEndCommit();
								break;
							} catch (RuntimeException e) {
								LOG.debug("Fail to insert", e.getMessage());
								retryCounter.incrementAndGet();
							}
						}
					}
					counter += n;
					LOG.debug("{} records inserted.", counter);
				}
			}
			LOG.debug("End insert task({} records inserted, retryCounter = {}).", counter, retryCounter.get());
		}
	}

	private class DeleteTask implements Runnable {
		private Config config;
		private TxOption txOption;
		private boolean skipDbAccess;

		public DeleteTask(Config config, TxOption txOption, boolean skipDbAccess) {
			this.config = config;
			this.txOption = txOption;
			this.skipDbAccess = skipDbAccess;
		}

		@Override
		public void run() {
			long counter = 0;
			LOG.debug("Start delete task");

			PhoneNumberGenerator phoneNumberGenerator = new PhoneNumberGenerator(config);

			try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
				HistoryDao dao  = manager.getHistoryDao();
				Timer timer = new Timer("DELETE", txOption == LTX ? "LTX" : "OCC");
				for (;;) {
					long basePhoneNumber = remaining.getAndAdd(-recordsPerCommit);
					if (basePhoneNumber <= 0) {
						break;
					}
					int n = (int) (basePhoneNumber > recordsPerCommit ? recordsPerCommit : basePhoneNumber);
					List<String> list = new ArrayList<String>(n);
					for (int i = 0; i < n; i++) {
						list.add(phoneNumberGenerator.to11DigtString(basePhoneNumber - i));
					}
					if (!skipDbAccess) {
						for (;;) {
							try {
								manager.execute(txOption, () -> {
									timer.setStartTx();
									for (String phoneNumber : list) {
										dao.delete(phoneNumber);
									}
									timer.setStartCommit();
								});
								timer.setEndCommit();
								break;
							} catch (RuntimeException e) {
								LOG.debug("Fail to delete", e.getMessage());
								retryCounter.incrementAndGet();
							}
						}
					}
					counter += n;
					LOG.debug("{} records deleteed.", counter);
				}
			}
			LOG.debug("End delete task({} records deleteed, retryCounter = {}).", counter, retryCounter.get());
		}
	}

	private class Record {
		private String description;
		private long start;
		private long elapsedMillis;
		private long numberOfRecords;
		private String vsz;
		private String rss;
		private int retryCount;

		public Record(String description) {
			this.description = description;
		}

		void start() {
			start = System.currentTimeMillis();
			LOG.info("Start {}.", description);
		}

		void end(long n, TateyamaWatcher tateyamaWatcher) {
			elapsedMillis = System.currentTimeMillis() - start;
			LOG.info("End {}, time = {} sec.", description, elapsedMillis / 1000.0);
			retryCount = retryCounter.get();
			numberOfRecords = n;
			vsz = convertToGB(tateyamaWatcher.getVsz());
			rss = convertToGB(tateyamaWatcher.getRss());
		}

		void print() {
			System.out.println(description + ", " + numberOfRecords + ", " + elapsedMillis / 1000.0 + "," + retryCount
					+ ", " + vsz + ", " + rss);
		}

		private String convertToGB(long bytes) {
		    double gb = (double) bytes / (1024 * 1024 * 1024);
		    return String.format("%.1fGB", gb);
		}
	}


	private static class Timer {
		String queryType;
		String txOption;

		Instant startTx = null;
		Instant startCommit = null;
		Instant endCommit = null;

		public Timer(String queryType, String txOption) {
			this.queryType = queryType;
			this.txOption = txOption;
		}

		public void setStartTx() {
			if (LOGGING_DETAIL_TIME_INFO) {
//				LOG.debug("setStartTx called");
				startTx = Instant.now();
			}
		}

		public void setStartCommit() {
			if (LOGGING_DETAIL_TIME_INFO) {
//				LOG.debug("setStartCommit called");
				startCommit = Instant.now();
			}
		}

		public void setEndCommit() {
			if (LOGGING_DETAIL_TIME_INFO) {
//				LOG.debug("setEndCommit called");
				endCommit = Instant.now();
				writeLog();
			}
		}

		private void writeLog() {
			if (LOGGING_DETAIL_TIME_INFO) {
				Duration d1 = Duration.between(startTx, startCommit);
				Duration d2 = Duration.between(startCommit, endCommit);
				LOG.debug("TIME INFO: {}\t{}\t{}\t{}\t{}", threadLabel, queryType, txOption,
						d1.toSeconds() * 1000 * 1000 + d1.toNanos() / 1000,  	// Durationをマイクロ秒で表示
						d2.toSeconds() * 1000 * 1000 + d2.toNanos() / 1000);	// Durationをマイクロ秒で表示
			}
		}
	}

}
