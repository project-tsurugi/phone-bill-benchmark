package com.tsurugidb.benchmark.phonebill.app;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
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
import com.tsurugidb.benchmark.phonebill.db.entity.Billing;
import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.benchmark.phonebill.testdata.CreateTestData;

/**
 * 引数でしていされた複数の設定でバッチを実行する。
 *
 */
public class MultipleExecute extends ExecutableCommand {
    private static final Logger LOG = LoggerFactory.getLogger(MultipleExecute.class);
	private List<Record> records = new ArrayList<>();
	private Set<History> expectedHistories;
	private Set<Billing> expectedBillings;

	public static void main(String[] args) throws Exception {
		MultipleExecute threadBench = new MultipleExecute();
		List<Config> configs = new ArrayList<>(args.length);
		for (String arg: args) {
			Config config = Config.getConfig(arg);
			configs.add(config);
		}
		threadBench.execute(configs);
	}

	@Override
	public void execute(List<Config> configs) throws Exception {
		for (Config config : configs) {
			new CreateTable().execute(config);
			new CreateTestData().execute(config);
			Record record = new Record(config);
			records.add(record);
			record.start();
			PhoneBill phoneBill = new PhoneBill();
			phoneBill.execute(config);
			record.finish(phoneBill.getTryCount(), phoneBill.getAbortCount());
			record.setNumberOfDiffrence(checkResult(config));
			writeResult(config);
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

	private int checkResult(Config config) {
		int n = 0;
		try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
			Set<History> histories = manager.execute(TxOption.ofRTX(0, "checkResult"), () -> {
				return new HashSet<>(manager.getHistoryDao().getHistories());
			});
			Set<Billing> billings = manager.execute(TxOption.ofRTX(0, "checkResult"), () -> {
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
		private int abortCount = 0;
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

		public void finish(int tryCount, int abortCount) {
			elapsedMillis = Duration.between(start, Instant.now()).toMillis();
			LOG.info("Finished phoneBill.exec(), elapsed secs = {}.", elapsedMillis / 1000.0);
			this.tryCount = tryCount;
			this.abortCount = abortCount;
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
			builder.append(elapsedMillis/1000.0);
			builder.append(",");
			builder.append(tryCount);
			builder.append(",");
			builder.append(abortCount);
			builder.append(",");
			builder.append(numberOfDiffrence);
			return builder.toString();
		}

		public static String header() {
			return "dbmsType, option, scope, threadCount, elapsedSeconds, tryCount, abortCount, diffrence";
		}

	}
}
