package com.tsurugidb.benchmark.phonebill.app;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config.DbmsType;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.TxLabel;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.dao.Ddl;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;

public class CreateTable extends ExecutableCommand{
    private static final Logger LOG = LoggerFactory.getLogger(CreateTable.class);

	public static void main(String[] args) throws Exception {
		Config config = Config.getConfig(args[0]);
		CreateTable createTable = new CreateTable();
		createTable.execute(config);
	}

	@Override
	public void execute(Config config) throws Exception {
		try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
			Ddl ddl = manager.getDdl();
			HistoryDao historyDao = manager.getHistoryDao();
			if (config.usePreparedTables && config.dbmsType == DbmsType.ICEAXE) {
				List<String> list = manager.execute(TxOption.ofRTX(Integer.MAX_VALUE, TxLabel.DDL),
						historyDao::getAllPhoneNumbers).stream().sorted().distinct().collect(Collectors.toList());
				if (list.size() != 0) {
					LOG.info("Deleting {} phone numbers from history table.", list.size());
					BlockingQueue<String> queue = new ArrayBlockingQueue<String>(list.size(), true, list);
					deleteHistories(config, queue);
				}
			}
			TxOption option = TxOption.ofOCC(Integer.MAX_VALUE, TxLabel.DDL);
			manager.execute(option, ddl::dropTables);
			manager.execute(option, ddl::createHistoryTable);
			manager.execute(option, ddl::createContractsTable);
			manager.execute(option, ddl::createBillingTable);
			manager.execute(option, ddl::createIndexes);
		}
	}

	private void deleteHistories(Config config, BlockingQueue<String> queue) throws InterruptedException, ExecutionException {
		int nThread = config.createTestDataThreadCount;
		ExecutorService service = Executors.newFixedThreadPool(nThread);
		List<Task> tasks;
		List<Future<Boolean>> futures= Collections.emptyList();
		try {
			tasks = IntStream.range(0, nThread).mapToObj(i -> new Task(config, queue))
					.collect(Collectors.toList());
			futures = tasks.stream().map(t -> service.submit(t)).collect(Collectors.toList());
		} finally {
			if (!service.isTerminated()) {
				service.shutdown();
			}
			boolean success = true;
			for (Future<Boolean> future : futures) {
				if (future.get() == false) {
					success = false;
				}
			}
			if (!success) {
				String msg = "Fail to create table.";
				LOG.error(msg);
				throw new RuntimeException(msg);
			}
		}
	}

	private static class Task implements Callable<Boolean> {
		private final Config config;
		private final BlockingQueue<String> queue;

		public Task(Config config, BlockingQueue<String> queue) {
			this.config = config;
			this.queue = queue;
		}


		@Override
		public Boolean call() throws Exception {
			try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
				HistoryDao dao = manager.getHistoryDao();
				String phoneNumber;
				while ((phoneNumber = queue.poll()) != null) {
					final String fstr = phoneNumber;
					try {
						manager.execute(TxOption.ofOCC(Integer.MAX_VALUE, TxLabel.DDL), () -> dao.delete(fstr));
					} catch (RuntimeException e) {
						LOG.error("Fail to delete history, caller_phone_number = {}", phoneNumber);
						return false;
					}
				}
			}
			return true;
		}
	}
}
