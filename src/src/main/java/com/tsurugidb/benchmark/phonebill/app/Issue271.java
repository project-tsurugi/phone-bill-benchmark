package com.tsurugidb.benchmark.phonebill.app;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.TxLabel;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.TxOption.Table;
import com.tsurugidb.benchmark.phonebill.db.dao.Ddl;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.PhoneBillDbManagerIceaxe;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.TableMetadata;
import com.tsurugidb.tsubakuro.sql.util.LoadBuilder;

public class Issue271 extends ExecutableCommand {
	private static final Logger LOG = LoggerFactory.getLogger(Issue271.class);
	private static final TxOption OCC = TxOption.ofOCC(0, TxLabel.TEST);
	private static final TxOption LTX = TxOption.ofLTX(0, TxLabel.TEST, Table.HISTORY);


	private Config config;
	private Queue<Path> queue = new ConcurrentLinkedQueue<>();


	@Override
	public void execute(Config config) throws Exception {
		this.config = config;
		initTable(config);
		execute(config, 32, LTX, true);
		execute(config, 32, LTX, false);
		execute(config, 32, OCC, true);
		execute(config, 32, OCC, false);
		execute(config, 16, LTX, true);
		execute(config, 12, LTX, true);
		execute(config, 8, LTX, true);
		execute(config, 6, LTX, true);
		execute(config, 4, LTX, true);
		execute(config, 3, LTX, true);
		execute(config, 2, LTX, true);
		execute(config, 1, LTX, true);
	}

	void execute(Config config, int threads, TxOption txOption, boolean sharedTx) {
		List<File> files = getParquetFiles(config);
		queue.addAll(files.stream().sorted().map(f -> f.toPath()).collect(Collectors.toList()));
		LOG.info("Load {} files from dir {}", files.size(), config.csvDir);
		long startTime = System.currentTimeMillis();

		try (PhoneBillDbManagerIceaxe manager = (PhoneBillDbManagerIceaxe) PhoneBillDbManagerIceaxe
				.createPhoneBillDbManager(config)) {
			List<Worker> wokers = new ArrayList<>();
			for (int i = 0; i < threads; i++) {
				Worker worker = new Worker(sharedTx ? manager : null, txOption);
				wokers.add(worker);
			}
			ExecutorService service = Executors.newFixedThreadPool(threads);
			Set<Future<Result>> futureSet = new HashSet<>();
			for (Worker worker : wokers) {
				Future<Result> future = service.submit(worker);
				futureSet.add(future);
			}
			LOG.info("All workers ware submitted.");
			waitForAllTaskEnd(service, futureSet);
		}

		long elapsedTime = System.currentTimeMillis() - startTime;
		String format = "Threads = %2d, TxOption = %s, sharedTx = %s ,elapsed sec = %,.3f sec ";
		LOG.info(String.format(format, threads, txOption, sharedTx, elapsedTime / 1000d));
	}

	private List<File> getParquetFiles(Config config) {
		Path dir = Path.of(config.csvDir);
		return Arrays.asList(dir.toFile().listFiles());
	}

	void initTable(Config config) {
		// テーブルの初期化
		try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
			LOG.info("init table start.");
			Ddl ddl = manager.getDdl();
			TxOption option = TxOption.ofOCC(Integer.MAX_VALUE, TxLabel.DDL);
			manager.execute(option, ()-> ddl.dropTable("history"));
			manager.execute(option, ddl::createHistoryTable);
			LOG.info("init table end.");
		}
	}

	void waitForAllTaskEnd(ExecutorService service, Set<Future<Result>> futureSet) {
		service.shutdown();
		while (!futureSet.isEmpty()) {
			Iterator<Future<Result>> it = futureSet.iterator();
			while (it.hasNext()) {
				Future<Result> future = it.next();
				if (future.isDone()) {
					Result result;
					try {
						result = future.get();
					} catch (InterruptedException | ExecutionException e) {
						result = new Result();
						result.success = false;
						result.e = e;
					}
					if (result.success) {
						LOG.info("Task exited normally");
					} else {
						LOG.error("Fail to load file: {}, aborting...", result.path, result.e);
						System.exit(1);
					}
					it.remove();
				}
			}
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				// Noting to do
			}
		}
	}



	/**
	 *
	 */
	private class Worker implements Callable<Result> {
		/**
		 * 使用するPhoneBillDbManager
		 */
		private PhoneBillDbManagerIceaxe manager;

		/**
		 * 使用するTxOption
		 */
		private TxOption txOption;


		public Worker(PhoneBillDbManagerIceaxe manager, TxOption txOption ) {
			this.manager = manager;
			this.txOption = txOption;
		}


		@Override
		public Result call() {
			Path path;
			Result result = new Result();
			boolean localTx = manager == null;
			while((path = queue.poll()) != null  ) {
				LOG.debug("Start to load file: {}", path);
				final Path fPath = path;
				try {
					LOG.debug("manager = {}", manager);
					if (localTx) {
						manager = (PhoneBillDbManagerIceaxe) PhoneBillDbManager.createPhoneBillDbManager(config);
					}
					LOG.debug("manager = {}", manager);
					try {
						manager.execute(txOption, () -> {
							try {
								com.tsurugidb.tsubakuro.sql.Transaction transaction = manager.getCurrentTransaction()
										.getLowTransaction();
								SqlClient client = manager.getSession().getLowSqlClient();
								TableMetadata tableMd = client.getTableMetadata("history").await();

								var cols = tableMd.getColumns();
								try (var load = LoadBuilder.loadTo(tableMd).style(LoadBuilder.Style.OVERWRITE)
										.mapping(cols.get(0), "caller_phone_number")
										.mapping(cols.get(1), "recipient_phone_number")
										.mapping(cols.get(2), "payment_category").mapping(cols.get(3), "start_time")
										.mapping(cols.get(4), "time_secs").mapping(cols.get(5), "charge")
										.mapping(cols.get(6), "df").build(client).await()) {
									LOG.debug("End LoadBuilder.loadTo()  for file: {}", fPath);
									load.submit(transaction, fPath).await();
									LOG.debug("End load.submit()  for file: {}", fPath);
								}

							} catch (IOException | InterruptedException | ServerException e) {
								throw new RuntimeException(e);
							}
						});
					} finally {
						if (localTx && manager != null) {
							manager.close();
							manager = null;
						}
					}
					LOG.debug("End to load file: {}", path);
				} catch (RuntimeException e) {
					result.success = false;
					result.e = e;
					result.path = path;
				}
			}
			return result;
		}
	}

	/**
	 * タスクの実行結果
	 *
	 */
	static class Result {
		Path path = null;
		boolean success = true;
		Exception e = null;
	}
}

