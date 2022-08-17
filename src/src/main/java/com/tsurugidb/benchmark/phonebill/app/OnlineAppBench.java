package com.tsurugidb.benchmark.phonebill.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.billing.PhoneBill;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.dao.Ddl;
import com.tsurugidb.benchmark.phonebill.testdata.CreateTestData;

/**
 * 以下の条件を変えて、バッチの処理時間がどう変化するのかを測定する
 * <ul>
 *   <li> オンラインアプリケーションを動かさない場合
 *   <li> 各オンラインアプリケーションを単独で動かした場合
 *   <li> すべてのオンラインアプリケーションを動かした場合
 * </ul>
 * 上記以外の値はConfigで指定された値を使用する
 *
 */
public class OnlineAppBench extends ExecutableCommand {
    private static final Logger LOG = LoggerFactory.getLogger(OnlineAppBench.class);
    private PhoneBillDbManager manager;


	public static void main(String[] args) throws Exception {
		OnlineAppBench threadBench = new OnlineAppBench();
		Config config = Config.getConfig(args);
		threadBench.execute(config);
	}


	@Override
	public void execute(Config config) throws Exception {
		manager = PhoneBillDbManager.createPhoneBillDbManager(config);
		int historyInsertTransactionPerMin = config.historyInsertTransactionPerMin;
		int historyUpdateRecordsPerMin = config.historyUpdateRecordsPerMin;
		int masterInsertReccrdsPerMin = config.masterInsertReccrdsPerMin;
		int masterUpdateRecordsPerMin = config.masterUpdateRecordsPerMin;
		long elapsedTime;

		// オンラインアプリケーションを動かさない場合
		config.historyInsertTransactionPerMin = 0;
		config.historyUpdateRecordsPerMin = 0;
		config.masterInsertReccrdsPerMin = 0;
		config.masterUpdateRecordsPerMin = 0;
		elapsedTime = execBatch(config);
		LOG.info("No online application, elapsed time = {} ms", elapsedTime);

		// 各オンラインアプリケーションを単独で動かした場合
		config.historyInsertTransactionPerMin = historyInsertTransactionPerMin;
		config.historyUpdateRecordsPerMin = 0;
		config.masterInsertReccrdsPerMin = 0;
		config.masterUpdateRecordsPerMin = 0;
		elapsedTime = execBatch(config);
		LOG.info("History insert online application, elapsed time = {} ms", elapsedTime);

		config.historyInsertTransactionPerMin = 0;
		config.historyUpdateRecordsPerMin = historyUpdateRecordsPerMin;
		config.masterInsertReccrdsPerMin = 0;
		config.masterUpdateRecordsPerMin = 0;
		elapsedTime = execBatch(config);
		LOG.info("History update online application, elapsed time = {} ms", elapsedTime);

		config.historyInsertTransactionPerMin = 0;
		config.historyUpdateRecordsPerMin = 0;
		config.masterInsertReccrdsPerMin = masterInsertReccrdsPerMin;
		config.masterUpdateRecordsPerMin = 0;
		elapsedTime = execBatch(config);
		LOG.info("Master insert online application, elapsed time = {} ms", elapsedTime);

		config.historyInsertTransactionPerMin = 0;
		config.historyUpdateRecordsPerMin = 0;
		config.masterInsertReccrdsPerMin = 0;
		config.masterUpdateRecordsPerMin = masterUpdateRecordsPerMin;
		elapsedTime = execBatch(config);
		LOG.info("Master update online application, elapsed time = {} ms", elapsedTime);

		// すべてのオンラインアプリケーションを動かした場合
		config.historyInsertTransactionPerMin = historyInsertTransactionPerMin;
		config.historyUpdateRecordsPerMin = historyUpdateRecordsPerMin;
		config.masterInsertReccrdsPerMin = historyUpdateRecordsPerMin;
		config.masterUpdateRecordsPerMin = masterUpdateRecordsPerMin;
		elapsedTime = execBatch(config);
		LOG.info("All online application, elapsed time = {} ms", elapsedTime);
	}


	/**
	 * 指定のconfigでバッチを実行し、処理時間を返す
	 *
	 * @param config
	 * @return
	 * @throws Exception
	 */
	private long execBatch(Config config) throws Exception {
		PhoneBill phoneBill = new PhoneBill();
		new CreateTable().execute(config);
		new CreateTestData().execute(config);
		beforeExec(config);
		phoneBill.execute(config);
		afterExec(config);
		return phoneBill.getElapsedTime();
	}


	private void afterExec(Config config) {
		Ddl executor = manager.getDdl();
		int historyUpdated = executor.countHistoryUpdated();
		int historyInserted = executor.count("history") - executor.count("history_back");
		int masterUpdated = executor.countContractsUpdated();
		int masterInserted = executor.count("contracts") - executor.count("contracts_back");
		LOG.info("history updated = " + historyUpdated);
		LOG.info("history inserted = " + historyInserted);
		LOG.info("master updated = " + masterUpdated);
		LOG.info("master inserted = " + masterInserted);

	}


	private void beforeExec(Config config) {
		Ddl executor = manager.getDdl();
		executor.dropTable("history_back");
		executor.dropTable("contracts_back");
		executor.createBackTable("history");
		executor.createBackTable("contracts");
	}
}
