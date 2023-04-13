package com.tsurugidb.benchmark.phonebill.app;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config.DbmsType;
import com.tsurugidb.benchmark.phonebill.app.Config.IsolationLevel;
import com.tsurugidb.benchmark.phonebill.app.Config.TransactionOption;
import com.tsurugidb.benchmark.phonebill.app.Config.TransactionScope;
import com.tsurugidb.benchmark.phonebill.app.billing.PhoneBill;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager.CounterKey;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager.CounterName;
import com.tsurugidb.benchmark.phonebill.db.TxLabel;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.dao.Ddl;
import com.tsurugidb.benchmark.phonebill.db.entity.Billing;
import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.benchmark.phonebill.testdata.CreateTestData;

/**
 * 引数でしていされた複数の設定でバッチを実行する。
 *
 */
public class MultipleExecute extends ExecutableCommand {
	private static final String ENV_NAME = "DB_INIT_CMD";

    private static final Logger LOG = LoggerFactory.getLogger(MultipleExecute.class);
	private List<Record> records = new ArrayList<>();
	private Set<History> expectedHistories;
	private Set<Billing> expectedBillings;
	private String onlineAppReport = "# Online Application Report \n\n";

	public static void main(String[] args) throws Exception {
		MultipleExecute threadBench = new MultipleExecute();
		List<ConfigInfo> configInfos = createConfigInfos(args, 0);
		threadBench.execute(configInfos);
	}

	@Override
	public void execute(List<ConfigInfo> configInfos) throws Exception {
		ExecutorService service = Executors.newFixedThreadPool(1);
		TateyamaWatcher task = null;
		Future<?> future = null;
		try {
			boolean prevConfigHasOnlineApp = false;
			for (ConfigInfo info : configInfos) {
				Config config = info.config;
				LOG.info("Using config {} " + System.lineSeparator() + "--- " + System.lineSeparator() + config
						+ System.lineSeparator() + "---", info.configPath.toAbsolutePath().toString());
				if (config.dbmsType.isTsurugi()) {
					dbiInit();
					task = new TateyamaWatcher();
					future = service.submit(task);
				}
				initTestData(config, prevConfigHasOnlineApp);
				Record record = new Record(config);
				records.add(record);
				record.start();
				PhoneBill phoneBill = new PhoneBill();
				phoneBill.execute(config);
				record.finish(phoneBill.getTryCount(), phoneBill.getAbortCount());
				record.setNumberOfDiffrence(checkResult(config));
				if (config.dbmsType.isTsurugi()) {
					LOG.info("Sending a request to stop TateyamaWatcher.");
					task.stop();
					future.get();
					LOG.info("TateyamaWatcher was stopped.");
					record.setMemInfo(task.getVsz(), task.getRss());
				}
				writeResult(config);
				if (config.hasOnlineApp()) {
					writeOnlineAppReport(config);
				}

				prevConfigHasOnlineApp = config.hasOnlineApp();
				PhoneBillDbManager.reportNotClosed();
			}
		} finally {
			if (task != null) {
				task.stop();
			}
			if (future != null) {
				future.get();
			}
			service.shutdown();
		}
	}

	/**
	 * テストデータを初期化する
	 *
	 * @param config
	 * @param prevConfigHasOnlineApp
	 * @throws Exception
	 */
	public void initTestData(Config config, boolean prevConfigHasOnlineApp) throws Exception {
		LOG.info("Starting test data generation.");
		new CreateTable().execute(config);
		new CreateTestData().execute(config);
		LOG.info("Test data generation has finished.");
	}

	public boolean needCreateTestData(Config config) {
		try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
			// テーブルの存在確認
			Ddl ddl = manager.getDdl();
			if (!ddl.tableExists("billing") || !ddl.tableExists("contracts") || !ddl.tableExists("history")) {
				return true;
			}
			long countHistory = manager.execute(TxOption.of(), () -> {
				return manager.getHistoryDao().count();
			});
			if (countHistory != config.numberOfHistoryRecords) {
				return true;
			}
			long countContracts = manager.execute(TxOption.of(), () -> {
				return manager.getContractDao().count();
			});
			if (countContracts != config.numberOfContractsRecords) {
				return true;
			}
		}
		return false;
	}


	/**
	 * 環境変数"DB_INIT_CMD"が設定されている場合、環境変数で指定されたコマンドを実行する
	 *
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void dbiInit() throws IOException, InterruptedException {
		LOG.info("Enter to dbInit().");
		String cmd = System.getenv(ENV_NAME);
		if (cmd == null || cmd.isEmpty()) {
			return;
		}
		LOG.info("Executing command: {}.", cmd);
		ProcessBuilder pb = new ProcessBuilder(cmd);
		pb.redirectErrorStream(true);
		Process p = pb.start();
		try (BufferedReader r = new BufferedReader(
				new InputStreamReader(p.getInputStream(), Charset.defaultCharset()))) {
			String line;
			while ((line = r.readLine()) != null) {
				System.out.println(line);
			}
		}
		int retCode = p.waitFor();
		String msg = cmd + " was terminated with exit code " + retCode + ".";
		LOG.info(msg);
		if (retCode != 0) {
			throw new RuntimeException(msg);
		}
	}


	/**
	 * 結果をCSVに出力する
	 *
	 * @param config
	 * @throws IOException
	 */
	private void writeResult(Config config) throws IOException {
		Path outputPath = Paths.get(config.csvDir).resolve("result.csv");
		try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(outputPath))) {
			pw.println(Record.header());
			records.stream().forEach(r -> pw.println(r.toString()));
		}
	}

	/**
	 * オンラインアプリのレポートを出力する
	 *
	 * @param config
	 * @param record
	 */
	private void writeOnlineAppReport(Config config) {
		// ex: ICEAXE-OCC-
		String title = config.dbmsType.name() + "-" + config.transactionOption + "-" + config.transactionScope + "-T"
				+ config.threadCount;
		Path outputPath = Paths.get(config.csvDir).resolve("online-app.md");
		try {
			LOG.debug("Creating an online application report for {}", title);
			String newReport = createOnlineAppReport(config, title);
			LOG.debug("Online application report: {}", newReport);
			onlineAppReport = onlineAppReport + newReport;
			LOG.debug("Writing online application reports to {}", outputPath.toAbsolutePath().toString());
			Files.writeString(outputPath, onlineAppReport);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}




	/**
	 * オンラインアプリのレポートを出力する
	 * </p>
	 * 出力サンプル
	 *
	 * <pre>
	 * ## ICEAXE-OCC-CONTRACT-T1
	 *
	 * | application    | Threads | tpm/thread | records/tx | succ | occ-try | occ-abort | occ-succ | occ abandoned retry | ltx-try | ltx-abort | ltx-succ |ltx abandoned retry|
	 * |----------------|--------:|-----------:|-----------:|-----:|--------:|----------:|---------:|--------------------:|--------:|----------:|---------:|------------------:|
	 * |master insert|1|-1|1|10137|10137|0|10137|0|0|0|0|0|
	 * |master update|1|-1|1|3747|3747|0|3747|0|0|0|0|0|
	 * |history insert|1|-1|100|354|382|42|340|14|18|4|14|0|
	 * |history update|1|-1|1|798|986|207|779|19|19|0|19|0|
	 * </pre>
	 *
	 * @param config
	 * @param title
	 * @return
	 */
	String createOnlineAppReport(Config config, String title) {
		StringBuilder sb = new StringBuilder();

		// タイトル
		sb.append("## " + title + "\n\n");

		// ヘッダ
		sb.append("| application    | Threads | tpm/thread | records/tx | succ | occ-try | occ-abort | occ-succ | occ<br>abandoned<br>retry | ltx-try | ltx-abort | ltx-succ |ltx<br>abandoned<br>retry|\n");
		sb.append("|----------------|--------:|-----------:|-----------:|-----:|--------:|----------:|---------:|--------------------------:|--------:|----------:|---------:|------------------------:|\n");


		// master insert
		OnlineAppRecord masterInsert = new OnlineAppRecord();
		masterInsert.application = "master insert";
		masterInsert.threads = config.masterInsertThreadCount;
		masterInsert.tpmTthread = config.masterInsertRecordsPerMin;
		masterInsert.recordsTx = 1;
		masterInsert.setCounterValues(TxLabel.MASTER_INSERT_APP);
		sb.append(masterInsert.toString());

		// master update
		OnlineAppRecord masterUpdate = new OnlineAppRecord();
		masterUpdate.application = "master update";
		masterUpdate.threads = config.masterUpdateThreadCount;
		masterUpdate.tpmTthread = config.masterUpdateRecordsPerMin;
		masterUpdate.recordsTx = 1;
		masterUpdate.setCounterValues(TxLabel.MASTER_UPDATE_APP);
		sb.append(masterUpdate.toString());

		// history insert
		OnlineAppRecord historyInsert = new OnlineAppRecord();
		historyInsert.application = "history insert";
		historyInsert.threads = config.historyInsertThreadCount;
		historyInsert.tpmTthread = config.historyInsertTransactionPerMin;
		historyInsert.recordsTx = config.historyInsertRecordsPerTransaction;
		historyInsert.setCounterValues(TxLabel.HISTORY_INSERT_APP);
		sb.append(historyInsert.toString());

		// history update
		OnlineAppRecord historyUpdate = new OnlineAppRecord();
		historyUpdate.application = "history update";
		historyUpdate.threads = config.historyUpdateThreadCount;
		historyUpdate.tpmTthread = config.historyUpdateRecordsPerMin;
		historyUpdate.recordsTx = 1;
		historyUpdate.setCounterValues(TxLabel.HISTORY_UPDATE_APP);
		sb.append(historyUpdate.toString());
		return sb.toString();
	}


	private int checkResult(Config config) {
		int n = 0;
		try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
			Set<History> histories = manager.execute(TxOption.ofRTX(0, TxLabel.CHECK_RESULT), () -> {
				List<History> list =manager.getHistoryDao().getHistories();
				list.stream().forEach(h -> h.setSid(0)); // sidは比較対象でないので0をセット
				return new HashSet<>(list);
			});
			Set<Billing> billings = manager.execute(TxOption.ofRTX(0, TxLabel.CHECK_RESULT), () -> {
				List<Billing> list = manager.getBillingDao().getBillings();
				list.stream().forEach(b -> b.setBatchExecId(null)); // batchExecIdは比較対象でないのでnullをセット
				return new HashSet<>(list);
			});
			if (expectedHistories == null) {
				expectedHistories = histories;
			} else {
				n += checkSameSet(expectedHistories, histories);
			}
			if (expectedBillings == null) {
				expectedBillings = billings;
			} else {
				n += checkSameSet(expectedBillings, billings);
			}
		}
		return n;
	}

	/**
	 * 二つのレコードを比較し差異のあるレコード数を返す
	 *
	 * @param <T>
	 * @param expect
	 * @param actual
	 * @return
	 */
	public static  <T> int checkSameSet(Set<T> expect, Set<T> actual) {
		int n = 0;
		for(T t: expect) {
			if (actual.contains(t)) {
				continue;
			}
			LOG.debug("only in expect:"  + t);
			n++;
		}
		for(T t: actual) {
			if (expect.contains(t)) {
				continue;
			}
			LOG.debug("only in actual:" + t);
			n++;
		}
		if (n != 0) {
			LOG.info("Did not get the same results.");
		}
		return n;
	}

	private static class Record {
		private TransactionOption option;
		private TransactionScope scope;
		private IsolationLevel isolationLevel;
		private int threadCount;
		private Instant start;
		private DbmsType dbmsType;
		private long elapsedMillis;
		private int tryCount = 0;
		private int abortCount = 0;
		private int numberOfDiffrence = 0;
		private long vsz = -1;
		private long rss = -1;
		private boolean hasOnlineApp;


		public Record(Config config) {
			this.option = config.transactionOption;
			this.scope = config.transactionScope;
			this.isolationLevel = config.isolationLevel;
			this.threadCount = config.threadCount;
			this.dbmsType = config.dbmsType;
			this.hasOnlineApp = config.hasOnlineApp();
		}

		public void start() {
			LOG.info("Executing phoneBill.exec() with {}.", getParamString());
			start = Instant.now();
		}

		public void finish(int tryCount, int abortCount) {
			elapsedMillis = Duration.between(start, Instant.now()).toMillis();
			LOG.info("Finished phoneBill.exec(), elapsed secs = {}.", elapsedMillis / 1000.0);
			this.tryCount = tryCount;
			this.abortCount = abortCount;
		}

		public void setNumberOfDiffrence(int num) {
			numberOfDiffrence = num;
		}


		public void setMemInfo(long vsz, long rss) {
			this.vsz = vsz;
			this.rss = rss;
		}



		private String getParamString() {
			StringBuilder builder = new StringBuilder();
			builder.append("dbmsType=");
			builder.append(dbmsType);
			builder.append(", option=");
			builder.append(dbmsType.isTsurugi() ? option : isolationLevel);
			builder.append(", scope=");
			builder.append(scope);
			builder.append(", threadCount=");
			builder.append(threadCount);
			return builder.toString();
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append(dbmsType);
			builder.append(",");
			builder.append(dbmsType.isTsurugi() ? option : isolationLevel);
			builder.append(",");
			builder.append(scope);
			builder.append(",");
			builder.append(threadCount);
			builder.append(",");
			builder.append(hasOnlineApp ? "Yes" : "No");
			builder.append(",");
			builder.append(String.format("%.3f", elapsedMillis / 1000.0));
			builder.append(",");
			builder.append(tryCount);
			builder.append(",");
			builder.append(abortCount);
			builder.append(",");
			builder.append(numberOfDiffrence);
			builder.append(",");
			builder.append(vsz == -1 ? "-" : String.format("%.1f", vsz / 1024f / 1024f / 1024f));
			builder.append(",");
			builder.append(rss == -1 ? "-" : String.format("%.1f", rss / 1024f / 1024f / 1024f));
			return builder.toString();
		}

		public static String header() {
			return "dbmsType, option, scope, threadCount, online app, elapsedSeconds, tryCount, abortCount, diffrence, vsz(GB), rss(GB)";
		}
	}


	private static class OnlineAppRecord {
		String application;
		int threads;
		int tpmTthread;
		int recordsTx;
		int succ;
		int occTry;
		int occAbort;
		int occSucc;
		int occAbandonedRtry;
		int ltxTry;
		int ltxAbort;
		int ltxSucc;
		int ltxAbandonedRtry;

		void setCounterValues(TxLabel txLabel) {
			occTry = PhoneBillDbManager.getCounter(CounterKey.of(txLabel, CounterName.OCC_TRY));
			occAbort = PhoneBillDbManager.getCounter(CounterKey.of(txLabel, CounterName.OCC_ABORT));
			occSucc = PhoneBillDbManager.getCounter(CounterKey.of(txLabel, CounterName.OCC_SUCC));
			occAbandonedRtry = PhoneBillDbManager.getCounter(CounterKey.of(txLabel, CounterName.OCC_ABANDONED_RETRY));
			ltxTry = PhoneBillDbManager.getCounter(CounterKey.of(txLabel, CounterName.LTX_TRY));
			ltxAbort = PhoneBillDbManager.getCounter(CounterKey.of(txLabel, CounterName.LTX_ABORT));
			ltxSucc = PhoneBillDbManager.getCounter(CounterKey.of(txLabel, CounterName.LTX_SUCC));;
			ltxAbandonedRtry = PhoneBillDbManager.getCounter(CounterKey.of(txLabel, CounterName.LTX_ABANDONED_RETRY));
			succ = occSucc + ltxSucc;
		}


		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("|");
			sb.append(application);
			sb.append("|");
			sb.append(threads);
			sb.append("|");
			sb.append(tpmTthread);
			sb.append("|");
			sb.append(recordsTx);
			sb.append("|");
			sb.append(succ);
			sb.append("|");
			sb.append(occTry);
			sb.append("|");
			sb.append(occAbort);
			sb.append("|");
			sb.append(occSucc);
			sb.append("|");
			sb.append(occAbandonedRtry);
			sb.append("|");
			sb.append(ltxTry);
			sb.append("|");
			sb.append(ltxAbort);
			sb.append("|");
			sb.append(ltxSucc);
			sb.append("|");
			sb.append(ltxAbandonedRtry);
			sb.append("|");
			sb.append("\n");
			return sb.toString();
		}
	}
}
