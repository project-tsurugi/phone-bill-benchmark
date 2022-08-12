package com.tsurugidb.benchmark.phonebill.online;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.AbstractJdbcTestCase;
import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.CreateTable;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.doma2.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.jdbc.DBUtils;
import com.tsurugidb.benchmark.phonebill.testdata.ContractBlockInfoAccessor;
import com.tsurugidb.benchmark.phonebill.testdata.SingleProcessContractBlockManager;
import com.tsurugidb.benchmark.phonebill.testdata.TestDataGenerator;

class MasterInsertAppTest extends AbstractJdbcTestCase {

	@Test
	void test() throws Exception {
		Config config = Config.getConfig();
		config.minDate = DBUtils.toDate("2010-01-11");
		config.maxDate = DBUtils.toDate("2020-12-21");
		config.numberOfContractsRecords = 10;
		config.expirationDateRate =3;
		config.noExpirationDateRate = 3;
		config.duplicatePhoneNumberRate = 2;

		// Generaterで10レコード作成する
		new CreateTable().execute(config);
		truncateTable("contracts");
		int seed = config.randomSeed;
		ContractBlockInfoAccessor accessor1 = new SingleProcessContractBlockManager();
		TestDataGenerator generator = new TestDataGenerator(config, new Random(seed), accessor1);
		try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
			generator.generateContractsToDb(manager);
		}
		List<Contract> expectedList = getContracts();

		// MasterInsertAppで5レコード生成し、Generatorで生成したレコードと一致することを確認
		truncateTable("contracts");

		ContractBlockInfoAccessor accessor2 = new SingleProcessContractBlockManager();
		MasterInsertApp app = new MasterInsertApp(config, new Random(seed), accessor2);
		app.exec();
		app.exec();
		app.exec();
		app.exec();
		app.exec();
		List<Contract> list = getContracts();
		assertEquals(5, list.size());
		for (int i = 0; i < 5; i++) {
			assertEquals(expectedList.get(i), list.get(i));
		}

		// 追加で5レコード生成して、Generatorで生成したレコードと一致することを確認
		app.exec();
		app.exec();
		app.exec();
		app.exec();
		app.exec();
		list = getContracts();
		assertEquals(10, list.size());
		assertEquals(expectedList, list);

	}


	protected List<Contract> getContracts() throws SQLException {
		List<Contract> list = new ArrayList<Contract>();
		String sql = "select phone_number, start_date, end_date, charge_rule from contracts";
		try (ResultSet rs = getStmt().executeQuery(sql)) {
			while (rs.next()) {
				Contract c = new Contract();
				c.phoneNumber = rs.getString(1);
				c.startDate = rs.getDate(2);
				c.endDate = rs.getDate(3);
				c.rule = rs.getString(4);
				list.add(c);
			}
		}
		return list;
	}

}
