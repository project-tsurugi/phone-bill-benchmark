package com.tsurugidb.benchmark.phonebill.app;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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



	private static final int[] THREAD_COUNTS = { 1, 4, 16, 64 };
	private static final TxOption OCC  =  TxOption.ofOCC(1, TxLabel.TEST);
	private static final TxOption LTX = TxOption.ofLTX(1, TxLabel.TEST, Table.HISTORY);
	private static final int RECORDS_PER_COMMIT = 1;

	private AtomicLong remaining;
	private List<Record> records = new ArrayList<>();


	public static void main(String[] args) throws Exception {
		Config config = Config.getConfig();
		config.numberOfHistoryRecords = 6400;
		new Issue220().execute(config);
	}

	@Override
	public void execute(Config config) throws Exception {
		// テーブルの作成
		try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
			Ddl ddl = manager.getDdl();
			manager.execute(TxOption.of(), () -> {
				ddl.dropTables();
				ddl.createHistoryTable();
			});
		}

		// スレッド数、TxOptionを変えて複数回実行する
		for (int threadCount : THREAD_COUNTS) {
			execute(config, threadCount, () -> {
				return new InsertTask(config, OCC, false);
			}, "Insert, OCC");
			execute(config, threadCount, () -> {
				return new DeleteTask(config, OCC, false);
			}, "Delete, OCC");
			execute(config, threadCount, () -> {
				return new InsertTask(config, LTX, false);
			}, "Insert, LTX");
			execute(config, threadCount, () -> {
				return new DeleteTask(config, LTX, false);
			}, "Delete, LTX");
			execute(config, threadCount, () -> {
				return new InsertTask(config, null, true);
			}, "Insert, NO_DB");
			execute(config, threadCount, () -> {
				return new DeleteTask(config, null, true);
			}, "Delete, NO_DB");
		}

		// レポート出力
		System.out.println("Type, Option, Threads, Time(sec)");
		for (Record record: records) {
			record.print();
		}
	}

	public void execute(Config config, int threadCount, Supplier<Runnable> supplier, String description)
			throws InterruptedException, ExecutionException {
		Record record = new Record(description + ", T" + threadCount);
		record.start();
		records.add(record);
		remaining = new AtomicLong(config.numberOfHistoryRecords);
		ExecutorService service = Executors.newFixedThreadPool(threadCount);
		List<Future<?>> futures = new ArrayList<>(threadCount);
		for (int i = 0; i < threadCount; i++) {
			Future<?> f = service.submit(supplier.get());
			futures.add(f);
		}
		service.shutdown();
		for(Future<?> f: futures) {
			f.get();
		}
		record.end();
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
				for (;;) {
					long basePhoneNumber = remaining.getAndAdd(-RECORDS_PER_COMMIT);
					if (basePhoneNumber <= 0) {
						break;
					}
					int n = (int) (basePhoneNumber > RECORDS_PER_COMMIT ? RECORDS_PER_COMMIT : basePhoneNumber);
					History h = new History();
					h.setRecipientPhoneNumber(phoneNumberGenerator.getPhoneNumber(0));
					h.setPaymentCategorty("C");
					h.setStartTime(LocalDateTime.now());
					h.setTimeSecs(100);
					h.setCharge(0);
					h.setDf(0);
					List<History> list = new ArrayList<History>(n);
					for (int i = 0; i < n; i++) {
						h.setCallerPhoneNumber(phoneNumberGenerator.getPhoneNumber(basePhoneNumber - i));
						list.add(h.clone());
					}
					if (!skipDbAccess) {
						manager.execute(txOption, () -> {
							dao.batchInsert(list);
						});
					}
					counter += n;
					LOG.debug("{} records inserted.", counter);
				}
			}
			LOG.debug("End insert task({} records inserted.", counter);
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
				for (;;) {
					long basePhoneNumber = remaining.getAndAdd(-RECORDS_PER_COMMIT);
					if (basePhoneNumber <= 0) {
						break;
					}
					int n = (int) (basePhoneNumber > RECORDS_PER_COMMIT ? RECORDS_PER_COMMIT : basePhoneNumber);
					List<String> list = new ArrayList<String>(n);
					for (int i = 0; i < n; i++) {
						list.add(phoneNumberGenerator.getPhoneNumber(basePhoneNumber - i));
					}
					if (!skipDbAccess) {
						manager.execute(txOption, () -> {
							for(String phoneNumber: list) {
								dao.delete(phoneNumber);
							}
						});
					}
					counter += n;
					LOG.debug("{} records deleteed.", counter);
				}
			}
			LOG.debug("End delete task({} records deleteed.", counter);
		}
	}

	private class Record {
		private String description;
		private long start;
		private long elapsedMillis;

		public Record(String description) {
			this.description = description;
		}

		void start() {
			start = System.currentTimeMillis();
			LOG.info("Start {}.", description);
		}

		void end() {
			elapsedMillis = System.currentTimeMillis() - start;
			LOG.info("End {}, time = {} sec.", description, elapsedMillis / 1000.0);
		}

		void print() {
			System.out.println(description + ", " +  elapsedMillis / 1000.0 );
		}
	}
}
