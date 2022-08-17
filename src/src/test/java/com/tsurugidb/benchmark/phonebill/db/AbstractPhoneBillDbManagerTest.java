package com.tsurugidb.benchmark.phonebill.db;

import java.io.IOException;
import java.util.Random;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import com.tsurugidb.benchmark.phonebill.AbstractJdbcTestCase;
import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.CreateTable;
import com.tsurugidb.benchmark.phonebill.db.jdbc.PhoneBillDbManagerJdbc;
import com.tsurugidb.benchmark.phonebill.testdata.ContractBlockInfoAccessor;
import com.tsurugidb.benchmark.phonebill.testdata.SingleProcessContractBlockManager;
import com.tsurugidb.benchmark.phonebill.testdata.TestDataGenerator;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "MS_CANNOT_BE_FINAL")
public class AbstractPhoneBillDbManagerTest extends AbstractJdbcTestCase {
	protected static final String ORACLE_CONFIG_PATH = "src/test/config/oracle.properties";

	protected static PhoneBillDbManagerJdbc managerOracle;
	protected static PhoneBillDbManagerJdbc managerPostgresql;
	protected static Config configOracle;
	protected static Config configPostgresql;

	@BeforeAll
	static void beforeAllTests() throws IOException {
		configOracle = Config.getConfig(ORACLE_CONFIG_PATH);
		configPostgresql = Config.getConfig();
		managerOracle = (PhoneBillDbManagerJdbc) PhoneBillDbManager.createPhoneBillDbManager(configOracle);
		managerPostgresql = (PhoneBillDbManagerJdbc) PhoneBillDbManagerJdbc.createPhoneBillDbManager(configPostgresql);
	}

	@AfterAll
	static void afterAllTests() {
		managerOracle.close();
		managerPostgresql.close();
	}

	protected void createTestData(Config config) throws IOException, Exception {
		config = config.clone();
		new CreateTable().execute(config);
		config.minDate = DateUtils.toDate("2010-01-11");
		config.maxDate = DateUtils.toDate("2020-12-21");
		config.numberOfContractsRecords = 10000;
		config.expirationDateRate =5;
		config.noExpirationDateRate = 11;
		config.duplicatePhoneNumberRate = 2;

		int seed = config.randomSeed;
		ContractBlockInfoAccessor accessor = new SingleProcessContractBlockManager();
		TestDataGenerator generator = new TestDataGenerator(config, new Random(seed), accessor);
		try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
			generator.generateContractsToDb(manager);
			generator.generateHistoryToDb(manager);
		}
	}
}
