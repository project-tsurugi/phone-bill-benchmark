package com.tsurugidb.benchmark.phonebill.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.billing.PhoneBill;
import com.tsurugidb.benchmark.phonebill.testdata.CreateTestData;

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

	public static void main(String[] args) throws Exception {
		Config config = Config.getConfig(args);
		ThreadBench threadBench = new ThreadBench();
		threadBench.execute(config);
	}


	@Override
	public void execute(Config config) throws Exception {
		new CreateTable().execute(config);
		new CreateTestData().execute(config);
		PhoneBill phoneBill = new PhoneBill();
		boolean[] sharedConnections = { false, true };
		int[] threadCounts = { 1, 2, 3, 4, 6, 8, 10, 15, 20, 25, 30, 40, 50, 60, 80, 90};
		for (boolean sharedConnection : sharedConnections) {
			for (int threadCount : threadCounts) {
				config.threadCount = threadCount;
				config.sharedConnection = sharedConnection;
				LOG.info("Executing phoneBill.exec() with threadCount = " + threadCount +
						", sharedConnection = " + sharedConnection);
				phoneBill.execute(config);
			}
		}
	}
}
