package com.tsurugidb.benchmark.phonebill.db;

import java.io.IOException;
import java.util.Random;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import com.tsurugidb.benchmark.phonebill.AbstractJdbcTestCase;
import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.Config.DbmsType;
import com.tsurugidb.benchmark.phonebill.app.CreateTable;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.PhoneBillDbManagerIceaxe;
import com.tsurugidb.benchmark.phonebill.db.jdbc.PhoneBillDbManagerJdbc;
import com.tsurugidb.benchmark.phonebill.db.postgresql.PhoneBillDbManagerPostgresqlNoBatchUpdate;
import com.tsurugidb.benchmark.phonebill.testdata.ContractBlockInfoAccessor;
import com.tsurugidb.benchmark.phonebill.testdata.SingleProcessContractBlockManager;
import com.tsurugidb.benchmark.phonebill.testdata.TestDataGenerator;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "MS_CANNOT_BE_FINAL")
public class AbstractPhoneBillDbManagerTest extends AbstractJdbcTestCase {
	private static final String ORACLE_CONFIG_PATH = "src/test/config/oracle.properties";
	private static final String ICEAXE_CONFIG_PATH = "src/test/config/iceaxe.properties";

	private static PhoneBillDbManagerJdbc managerOracle;
	private static PhoneBillDbManagerJdbc managerPostgresql;
	private static PhoneBillDbManagerIceaxe managerIceaxe;
	private static PhoneBillDbManagerPostgresqlNoBatchUpdate phoneBillDbManagerPostgresqlNoBatchUpdate;
	private static Config configOracle;
	private static Config configPostgresql;
	private static Config configIceaxe;
	private static Config configPostgresqlNoBatchUpdate;

	@BeforeAll
	static void beforeAllTests() throws IOException {
		configOracle = Config.getConfig(ORACLE_CONFIG_PATH);
		configPostgresql = Config.getConfig();
		configIceaxe = Config.getConfig(ICEAXE_CONFIG_PATH);
		configPostgresqlNoBatchUpdate = configPostgresql.clone();
		configPostgresqlNoBatchUpdate.dbmsType = DbmsType.POSTGRE_NO_BATCHUPDATE;
	}

	@AfterAll
	static void afterAllTests() {
		if (managerOracle != null) {
			managerOracle.close();
		}
		if (managerPostgresql != null) {
			managerPostgresql.close();
		}
		if (managerIceaxe != null) {
			managerIceaxe.close();
		}
		if (phoneBillDbManagerPostgresqlNoBatchUpdate != null) {
			phoneBillDbManagerPostgresqlNoBatchUpdate.close();
		}
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
		}
		generator.generateHistoryToDb(config);
	}

	protected static synchronized PhoneBillDbManagerJdbc getManagerOracle() {
		if (managerOracle == null) {
			managerOracle = (PhoneBillDbManagerJdbc) PhoneBillDbManager.createPhoneBillDbManager(getConfigOracle());
		}
		return managerOracle;
	}

	protected static synchronized PhoneBillDbManagerJdbc getManagerPostgresql() {
		if (managerPostgresql == null) {
			managerPostgresql = (PhoneBillDbManagerJdbc) PhoneBillDbManager.createPhoneBillDbManager(getConfigPostgresql());
		}
		return managerPostgresql;
	}

	protected static synchronized PhoneBillDbManagerPostgresqlNoBatchUpdate getManagerPostgresqlNoBatchupdate() {
		if (phoneBillDbManagerPostgresqlNoBatchUpdate == null) {
			phoneBillDbManagerPostgresqlNoBatchUpdate = (PhoneBillDbManagerPostgresqlNoBatchUpdate) PhoneBillDbManager
					.createPhoneBillDbManager(getConfigPostgresqlNoBatchUpdate());
		}
		return phoneBillDbManagerPostgresqlNoBatchUpdate;
	}

	protected static synchronized PhoneBillDbManagerIceaxe getManagerIceaxe() {
		if (managerIceaxe == null) {
			managerIceaxe = (PhoneBillDbManagerIceaxe) PhoneBillDbManager.createPhoneBillDbManager(getConfigIceaxe());
		}
		return managerIceaxe;
	}


	protected static  Config getConfigOracle() {
		return configOracle;
	}

	protected static Config getConfigPostgresql() {
		return configPostgresql;
	}

	protected static Config getConfigIceaxe() {
		return configIceaxe;
	}

	protected static Config getConfigPostgresqlNoBatchUpdate() {
		return configPostgresqlNoBatchUpdate;
	}
}
