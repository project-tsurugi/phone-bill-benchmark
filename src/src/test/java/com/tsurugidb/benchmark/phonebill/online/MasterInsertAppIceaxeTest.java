package com.tsurugidb.benchmark.phonebill.online;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.CreateTable;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.IceaxeTestTools;
import com.tsurugidb.benchmark.phonebill.testdata.ContractBlockInfoAccessor;
import com.tsurugidb.benchmark.phonebill.testdata.SingleProcessContractBlockManager;
import com.tsurugidb.benchmark.phonebill.testdata.TestDataGenerator;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;

class MasterInsertAppIceaxeTest {
	private static String ICEAXE_CONFIG = "src/test/config/iceaxe.properties";

	@Test
	void test() throws Exception {
		Config config = Config.getConfig(ICEAXE_CONFIG);
		config.minDate = DateUtils.toDate("2010-01-11");
		config.maxDate = DateUtils.toDate("2020-12-21");
		config.numberOfContractsRecords = 10;
		config.expirationDateRate = 3;
		config.noExpirationDateRate = 3;
		config.duplicatePhoneNumberRate = 2;
		try (IceaxeTestTools testTools = new IceaxeTestTools(config)) {

			// Generaterで10レコード作成する
			new CreateTable().execute(config);
			testTools.truncateTable("contracts");
			int seed = config.randomSeed;
			ContractBlockInfoAccessor accessor1 = new SingleProcessContractBlockManager();
			TestDataGenerator generator = new TestDataGenerator(config, new Random(seed), accessor1);
			try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
				generator.generateContractsToDb(manager);
			}
			List<Contract> expectedList = testTools.getContractList();

			// MasterInsertAppで5レコード生成し、Generatorで生成したレコードと一致することを確認
			testTools.truncateTable("contracts");

			ContractBlockInfoAccessor accessor2 = new SingleProcessContractBlockManager();
			MasterInsertApp app = new MasterInsertApp(config, new Random(seed), accessor2);
			try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
				app.exec(manager);
				app.exec(manager);
				app.exec(manager);
				app.exec(manager);
				app.exec(manager);
				List<Contract> list = testTools.getContractList();
				assertEquals(5, list.size());
				for (int i = 0; i < 5; i++) {
					assertEquals(expectedList.get(i), list.get(i));
				}

				// 追加で5レコード生成して、Generatorで生成したレコードと一致することを確認
				app.exec(manager);
				app.exec(manager);
				app.exec(manager);
				app.exec(manager);
				app.exec(manager);
				list = testTools.getContractList();
				assertEquals(10, list.size());
				assertEquals(expectedList, list);
			}
		}
	}
}
