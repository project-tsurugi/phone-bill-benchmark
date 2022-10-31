package com.tsurugidb.benchmark.phonebill.app;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config.DbmsType;
import com.tsurugidb.benchmark.phonebill.app.Config.IsolationLevel;
import com.tsurugidb.benchmark.phonebill.app.Config.TransactionOption;
import com.tsurugidb.benchmark.phonebill.app.Config.TransactionScope;
import com.tsurugidb.benchmark.phonebill.app.billing.PhoneBill;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;
import com.tsurugidb.benchmark.phonebill.db.entity.Billing;
import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.benchmark.phonebill.testdata.CreateTestData;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;

/**
 * スレッド数とコネクション共有の有無で、PhoneBillコマンドの実行時間がどう変化するのかを調べる
 * ためのコマンド.
 *
 * threadCounts, sharedConnection以外の設定値はコンフィグレーションファイルで指定された
 * 値を使用する。
 *
 */
public class ThreadBench extends ExecutableCommand {
    private static final Logger LOG = LoggerFactory.getLogger(ThreadBench.class);
	private List<Record> records = new ArrayList<>();
	private Set<History> expectedHistories;
	private Set<Billing> expectedBillings;

	public static void main(String[] args) throws Exception {
		ThreadBench threadBench = new ThreadBench();
		for (String arg: args) {
			Config config = Config.getConfig(arg);
			threadBench.execute(config);
		}
	}

	@Override
	public void execute(Config config) throws Exception {
		List<TransactionOption> options;
		List<TransactionScope> scopes;
		List<Integer> threadCounts;
		List<IsolationLevel> isolationLevels;

		if (config.dbmsType == DbmsType.ICEAXE) {
			options = Arrays.asList(TransactionOption.LTX, TransactionOption.OCC);
			scopes = Arrays.asList(TransactionScope.CONTRACT, TransactionScope.WHOLE);
			threadCounts = Arrays.asList(1, 2, 4, 6);
			isolationLevels = Collections.singletonList(IsolationLevel.SERIALIZABLE);
		} else {
			options = Collections.singletonList(TransactionOption.OCC);
			scopes = Arrays.asList(TransactionScope.CONTRACT, TransactionScope.WHOLE);
			threadCounts = Arrays.asList(1, 2, 4, 6);
			isolationLevels = Arrays.asList(IsolationLevel.SERIALIZABLE, IsolationLevel.READ_COMMITTED);
		}
		execute(config, options, scopes, isolationLevels, threadCounts);

		// 結果の出力
		Path outputPath = Paths.get(config.csvDir).resolve("result.csv");
		try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(outputPath))) {
			pw.println(Record.header());
			records.stream().forEach(r -> pw.println(r.toString()));
		}
	}

	private void execute(Config config, List<TransactionOption> options, List<TransactionScope> scopes,
			List<IsolationLevel> isolationLevels,
			List<Integer> threadCounts) throws Exception {
		new CreateTable().execute(config);
		new CreateTestData().execute(config);
		for (int threadCount : threadCounts) {
			for (TransactionOption option : options) {
				for (IsolationLevel isolationLevel : isolationLevels) {
					for (TransactionScope scope : scopes) {
						config.threadCount = threadCount;
						config.sharedConnection = false;
						config.transactionOption = option;
						config.transactionScope = scope;
						config.isolationLevel = isolationLevel;
						prepare(config);
						Record record = new Record(config);
						records.add(record);
						record.start();
						PhoneBill phoneBill = new PhoneBill();
						phoneBill.execute(config);
						record.finish(phoneBill.getTryCount());
						record.setNumberOfDiffrence(checkResult(config));
					}
				}
			}
		}
	}


	private void prepare(Config config) {
		try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
			HistoryDao dao = manager.getHistoryDao();
			manager.execute(TxOption.of(TgTmSetting.of(TgTxOption.ofOCC())), dao::updateChargeNull);
		}
	}

	private int checkResult(Config config) {
		int n = 0;
		try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
			Set<History> histories = manager.execute(TxOption.of(TgTmSetting.of(TgTxOption.ofOCC())), () -> {
				return new HashSet<>(manager.getHistoryDao().getHistories());
			});
			Set<Billing> billings = manager.execute(TxOption.of(TgTmSetting.of(TgTxOption.ofOCC())), () -> {
				List<Billing> list = manager.getBillingDao().getBillings();
				list.stream().forEachOrdered(b -> b.setBatchExecId(null)); // batchExecIdは比較対象でないのでnullをセット
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
			LOG.info("only in expect:"  + t);
			n++;
		}
		for(T t: actual) {
			if (expect.contains(t)) {
				continue;
			}
			LOG.info("only in actual:" + t);
			n++;
		}
		if (n != 0) {
			LOG.error("Did not get the same results.");
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
		private int numberOfDiffrence = 0;

		public Record(Config config) {
			this.option = config.transactionOption;
			this.scope = config.transactionScope;
			this.isolationLevel = config.isolationLevel;
			this.threadCount = config.threadCount;
			this.dbmsType = config.dbmsType;
		}

		public void start() {
			LOG.info("Executing phoneBill.exec() with {}.", getParamString());
			start = Instant.now();
		}

		public void finish(int tryCount) {
			elapsedMillis = Duration.between(start, Instant.now()).toMillis();
			LOG.info("Finished phoneBill.exec(), elapsed secs = {}.", elapsedMillis / 1000.0);
			this.tryCount = tryCount;
		}

		public void setNumberOfDiffrence(int num) {
			numberOfDiffrence = num;
		}

		private String getParamString() {
			StringBuilder builder = new StringBuilder();
			builder.append("dbmsType=");
			builder.append(dbmsType);
			builder.append(", option=");
			builder.append(dbmsType == DbmsType.ICEAXE ? option : isolationLevel);
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
			builder.append(dbmsType == DbmsType.ICEAXE ? option : isolationLevel);
			builder.append(",");
			builder.append(scope);
			builder.append(",");
			builder.append(threadCount);
			builder.append(",");
			builder.append(elapsedMillis);
			builder.append(",");
			builder.append(tryCount);
			builder.append(",");
			builder.append(numberOfDiffrence);
			return builder.toString();
		}

		public static String header() {
			return "dbmsType, option, scope, threadCount, elapsedMills, tryCount, diffrence";
		}

	}
}
