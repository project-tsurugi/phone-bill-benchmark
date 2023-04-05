package com.tsurugidb.benchmark.phonebill.app;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.TxLabel;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.TxOption.Table;
import com.tsurugidb.benchmark.phonebill.db.dao.Ddl;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;
import com.tsurugidb.benchmark.phonebill.testdata.ContractBlockInfoAccessor;
import com.tsurugidb.benchmark.phonebill.testdata.SingleProcessContractBlockManager;
import com.tsurugidb.benchmark.phonebill.testdata.TestDataGenerator;


public class Issue207 extends ExecutableCommand {
	private static final Logger LOG = LoggerFactory.getLogger(Issue207.class);

	@Override
	public void execute(Config config) throws Exception {
		// テーブルを作成

		new CreateTable().execute(config);

		config.isolationLevel = Config.IsolationLevel.READ_COMMITTED;
		String format;
		long startTime, elapsedTime;

		try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
			Ddl ddl = manager.getDdl();
			HistoryDao historyDao = manager.getHistoryDao();

			// TestDataGeneratorの初期化
			int seed = config.randomSeed;
			ContractBlockInfoAccessor accessor = new SingleProcessContractBlockManager();
			TestDataGenerator generator = new TestDataGenerator(config, new Random(seed), accessor);

			// 契約マスタのテストデータ生成
			startTime = System.currentTimeMillis();
			generator.generateContractsToDb(manager);
			elapsedTime = System.currentTimeMillis() - startTime;
			format = "%,d records generated to contracts table in %,.3f sec ";
			LOG.info(String.format(format, config.numberOfContractsRecords, elapsedTime / 1000d));

			// 通話履歴のテストデータを作成
			startTime = System.currentTimeMillis();
			generator.generateHistoryToDb(config);
			elapsedTime = System.currentTimeMillis() - startTime;
			format = "%,d records generated to history table in %,.3f sec ";
			LOG.info(String.format(format, config.numberOfHistoryRecords, elapsedTime / 1000d));

			// ヒストリテーブルのデータを削除
			LOG.info("Deleting table history.");
			manager.execute(TxOption.ofLTX(0, TxLabel.DDL, Table.HISTORY), () -> historyDao.delete());

//			LOG.info(": Sleeping for 10 seconds.");
//			Thread.sleep(10000);

			// Contractsテーブルの初期化(drop -> create)
			TxOption option = TxOption.ofOCC(0, TxLabel.DDL);
			LOG.info("Dropping table contracts.");
			manager.execute(option, () -> {
				ddl.dropTable("contracts");
			});
			option = TxOption.ofOCC(0, TxLabel.DDL);
			LOG.info("Creating table contracts.");
			manager.execute(option, ddl::createContractsTable);
			LOG.info("Creating table contracts completed.");
		}
	}

}
