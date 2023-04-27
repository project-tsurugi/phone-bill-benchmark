package com.tsurugidb.benchmark.phonebill.app;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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

	@Override
	public void execute(Config config) throws Exception {
		initTable(config);

		List<File> files = getParquetFiles(config);
		Collections.sort(files);
		LOG.info("Load {} files from dir {}", files.size(), config.csvDir);


		try (PhoneBillDbManagerIceaxe manager = (PhoneBillDbManagerIceaxe) PhoneBillDbManagerIceaxe
				.createPhoneBillDbManager(config)) {

			List<LoadTask> tasks = new ArrayList<>();
			for (File file : files) {
				LoadTask task = new LoadTask(file.toPath(), manager, OCC);
				tasks.add(task);
			}
			ExecutorService service = Executors.newFixedThreadPool(8);
			Set<Future<Result>> futureSet = new HashSet<>();
			for (LoadTask task : tasks) {
				Future<Result> future = service.submit(task);
				futureSet.add(future);
			}
			LOG.info("All tasks ware submitted.");
			waitForAllTaskEnd(service, futureSet);
		}
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
						result = new Result(null);
						result.success = false;
						result.e = e;
					}
					if (result.success) {
						LOG.info("Task success: {}", result.path);
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
	private class LoadTask implements Callable<Result> {
		/**
		 * ロードするファイルのpath
		 */
		private Path path;

		/**
		 * 使用するPhoneBillDbManager
		 */
		private PhoneBillDbManagerIceaxe manager;

		/**
		 * 使用するTxOption
		 */
		private TxOption txOption;


		public LoadTask(Path path, PhoneBillDbManagerIceaxe manager, TxOption txOption ) {
			this.path = path;
			this.manager = manager;
			this.txOption = txOption;
		}


		@Override
		public Result call() {
			LOG.info("Start to load file: {}", path);
			Result result = new Result(path);
			try  {
				manager.execute(txOption, () -> {
					try {
						com.tsurugidb.tsubakuro.sql.Transaction transaction = manager.getCurrentTransaction()
								.getLowTransaction();
						SqlClient client = manager.getSession().getLowSqlClient();
						TableMetadata tableMd = client.getTableMetadata("history").await();

						var cols = tableMd.getColumns();
						try (var load = LoadBuilder.loadTo(tableMd)
								.style(LoadBuilder.Style.OVERWRITE)
								.mapping(cols.get(0), "caller_phone_number")
								.mapping(cols.get(1), "recipient_phone_number")
								.mapping(cols.get(2), "payment_category")
								.mapping(cols.get(3), "start_time")
								.mapping(cols.get(4), "time_secs")
								.mapping(cols.get(5), "charge")
								.mapping(cols.get(6), "df").build(client).await()) {
							load.submit(transaction, Path.of("/home/umegane/pfile/d1683011587_0_0.parquet")).await();
						}

					} catch (IOException | InterruptedException | ServerException e) {
						throw new RuntimeException(e);
					}
					LOG.info("End to load file: {}", path);
				});
			} catch (RuntimeException e) {
				result.success = false;
				result.e = e;
			}
			return result;
		}
	}

	/**
	 * タスクの実行結果
	 *
	 */
	static class Result {
		Path path;
		boolean success = true;
		Exception e = null;

		public Result(Path path) {
			this.path = path;
		}
	}
}

