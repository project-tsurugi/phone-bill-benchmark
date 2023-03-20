package com.tsurugidb.benchmark.phonebill.app;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.dao.Ddl;

class MultipleExecuteTest {
	private static final String ICEAXE_CONFIG_PATH = "src/test/config/iceaxe.properties";

	@Test
	final void testNeedCreateTestData() throws Exception {
		// デフォルトのDB(PostgreSQLでのテスト
		testNeedCreateTestDataSub(Config.getConfig());
		// Iceaxeでのテスト
		testNeedCreateTestDataSub(Config.getConfig(ICEAXE_CONFIG_PATH));
	}

	private void testNeedCreateTestDataSub(Config config) throws Exception {
		Config testConfig = config.clone();
		new CreateTable().execute(config);

		testConfig.numberOfContractsRecords = 0;
		testConfig.numberOfHistoryRecords = 0;

		MultipleExecute multipleExecute = new MultipleExecute();
		assertFalse(multipleExecute.needCreateTestData(testConfig));


		try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
			Ddl ddl = manager.getDdl();

			// history
			manager.execute(TxOption.of(), () -> ddl.dropTable("history"));
			assertTrue(multipleExecute.needCreateTestData(testConfig));
			manager.execute(TxOption.of(), ddl::createHistoryTable);
			assertFalse(multipleExecute.needCreateTestData(testConfig));

			// billing
			manager.execute(TxOption.of(), () -> ddl.dropTable("billing"));
			assertTrue(multipleExecute.needCreateTestData(testConfig));
			manager.execute(TxOption.of(), ddl::createBillingTable);
			assertFalse(multipleExecute.needCreateTestData(testConfig));


			// contracts
			manager.execute(TxOption.of(), () -> ddl.dropTable("contracts"));
			assertTrue(multipleExecute.needCreateTestData(testConfig));
			manager.execute(TxOption.of(), ddl::createContractsTable);
			assertFalse(multipleExecute.needCreateTestData(testConfig));
		}

		testConfig.numberOfContractsRecords = 1;
		assertTrue(multipleExecute.needCreateTestData(testConfig));
		testConfig.numberOfContractsRecords = 0;
		assertFalse(multipleExecute.needCreateTestData(testConfig));

		testConfig.numberOfHistoryRecords = 1;
		assertTrue(multipleExecute.needCreateTestData(testConfig));
		testConfig.numberOfHistoryRecords = 0;
		assertFalse(multipleExecute.needCreateTestData(testConfig));
	}

}
