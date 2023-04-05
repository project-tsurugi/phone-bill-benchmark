package com.tsurugidb.benchmark.phonebill.testdata;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.CreateTable;
import com.tsurugidb.benchmark.phonebill.app.ExecutableCommand;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.TxLabel;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.dao.Ddl;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class CreateTestData extends ExecutableCommand {
    private static final Logger LOG = LoggerFactory.getLogger(CreateTestData.class);

    public static void main(String[] args) throws Exception {
		Config config = Config.getConfig(args);
		CreateTestData createTestData = new CreateTestData();
		createTestData.execute(config);
	}


	@Override
	@SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
	public void execute(Config c) throws Exception {
		// テストデータの作成時は、configの指定にかかわらずTRANSACTION_READ_COMMITTEDを使用する。
		Config config = c.clone();
		config.isolationLevel = Config.IsolationLevel.READ_COMMITTED;
		try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
			Ddl ddl = manager.getDdl();

			// テーブルを作成
			new CreateTable().execute(config);

			// インデックスの削除
			manager.execute(TxOption.ofOCC(Integer.MAX_VALUE, TxLabel.DDL), () -> {
				ddl.prepareLoadData();
			});

			// TestDataGeneratorの初期化
			int seed = config.randomSeed;
			ContractBlockInfoAccessor accessor = new SingleProcessContractBlockManager();
			TestDataGenerator generator = new TestDataGenerator(config, new Random(seed), accessor);

			// 契約マスタのテストデータ生成
			long startTime = System.currentTimeMillis();
			generator.generateContractsToDb(manager);
			long elapsedTime = System.currentTimeMillis() - startTime;
			String format = "%,d records generated to contracts table in %,.3f sec ";
			LOG.info(String.format(format, config.numberOfContractsRecords, elapsedTime / 1000d));

			// 通話履歴のテストデータを作成
			startTime = System.currentTimeMillis();
			generator.generateHistoryToDb(config);
			elapsedTime = System.currentTimeMillis() - startTime;
			format = "%,d records generated to history table in %,.3f sec ";
			LOG.info(String.format(format, config.numberOfHistoryRecords, elapsedTime / 1000d));

			// Indexの再生成とDBの統計情報を更新
			manager.execute(TxOption.ofOCC(Integer.MAX_VALUE, TxLabel.DDL), () -> {
				ddl.afterLoadData();
			});
		}
	}
}
