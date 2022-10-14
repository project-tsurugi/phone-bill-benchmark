package com.tsurugidb.benchmark.phonebill.app;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config.DbmsType;
import com.tsurugidb.benchmark.phonebill.app.Config.TransactionOption;
import com.tsurugidb.benchmark.phonebill.app.Config.TransactionScope;
import com.tsurugidb.benchmark.phonebill.app.billing.PhoneBill;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
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
		TransactionOption[] options = { TransactionOption.LTX, TransactionOption.OCC };
		TransactionScope[] scopes = { TransactionScope.CONTRACT, TransactionScope.WHOLE };
		int[] threadCounts = { 1, 2, 3, 4, 6, 8, 10, 15, 20 };

		execute(config, options, scopes, threadCounts);

		// 結果の出力
		LOG.info(Record.header());
		records.stream().forEach(r -> LOG.info(r.toString()));
	}


	private void execute(Config config, TransactionOption[] options, TransactionScope[] scopes, int[] threadCounts)
			throws Exception {
		new CreateTable().execute(config);
		new CreateTestData().execute(config);
		PhoneBill phoneBill = new PhoneBill();
		for (int threadCount : threadCounts) {
			for (TransactionOption option : options) {
				for (TransactionScope scope : scopes) {
					config.sharedConnection = false;
					config.threadCount = threadCount;
					config.transactionOption = option;
					config.transactionScope = scope;
					Record record = new Record(config);
					records.add(record);
					record.start();
					phoneBill.execute(config);
					record.finish();
					checkResult(config);
				}
			}
		}
	}




	private void checkResult(Config config) {
		PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config);
		Set<History> histories = manager.execute(TgTmSetting.of(TgTxOption.ofOCC()), () -> {
			return new HashSet<>(manager.getHistoryDao().getHistories());
		});
		Set<Billing> billings = manager.execute(TgTmSetting.of(TgTxOption.ofOCC()), () -> {
			List<Billing> list = manager.getBillingDao().getBillings();
			list.stream().forEachOrdered(b -> b.setBatchExecId(null)); // batchExecIdは比較対象でないのでnullをセット
			return new HashSet<>(list);
		});
		if (expectedHistories == null) {
			expectedHistories = histories;
		} else {
			checkSameSet(expectedHistories, histories);
		}
		if (expectedBillings == null) {
			expectedBillings = billings;
		} else {
			checkSameSet(expectedBillings, billings);
		}
	}

	private <T> void checkSameSet(Set<T> expect, Set<T> actual) {
		boolean ok = true;
		for(T t: expect) {
			if (actual.contains(t)) {
				continue;
			}
			LOG.info("only in expect; " + t.hashCode() + ":"  + t);
			ok = false;
		}
		for(T t: actual) {
			if (expect.contains(t)) {
				continue;
			}
			LOG.info("only in actual; " + t.hashCode() + ":" + t);
			ok = false;
		}
		if (!ok) {
			throw new RuntimeException("Did not get the same results.");
		}
	}

	private static class Record {
		private TransactionOption option;
		private TransactionScope scope;
		private int threadCount;
		private Instant start;
		private DbmsType dbmsType;
		private long elapsedMillis;

		public Record(Config config) {
			this.option = config.transactionOption;
			this.scope = config.transactionScope;
			this.threadCount = config.threadCount;
			this.dbmsType = config.dbmsType;
		}

		public void start() {
			LOG.info("Executing phoneBill.exec() with {}.", getParamString());
			start = Instant.now();
		}

		public void finish() {
			elapsedMillis = Duration.between(start, Instant.now()).toMillis();
			LOG.info("Finished phoneBill.exec(), elapsed secs = {}.", elapsedMillis / 1000.0);
		}


		private String getParamString() {
			StringBuilder builder = new StringBuilder();
			builder.append("dbmsType=");
			builder.append(dbmsType);
			builder.append(", option=");
			builder.append(option);
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
			builder.append(option);
			builder.append(",");
			builder.append(scope);
			builder.append(",");
			builder.append(threadCount);
			builder.append(",");
			builder.append(elapsedMillis);
			return builder.toString();
		}

		public static String header() {
			return "option, scope, threadCount, elapsedSec";
		}

	}
}
