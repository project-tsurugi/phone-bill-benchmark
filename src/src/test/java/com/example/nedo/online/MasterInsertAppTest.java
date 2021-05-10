package com.example.nedo.online;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

import com.example.nedo.AbstractDbTestCase;
import com.example.nedo.app.Config;
import com.example.nedo.app.CreateTable;
import com.example.nedo.db.Contract;
import com.example.nedo.db.DBUtils;
import com.example.nedo.testdata.TestDataGenerator;

class MasterInsertAppTest extends AbstractDbTestCase {

	@Test
	void test() throws Exception {
		Config config = Config.getConfig();
		config.minDate = DBUtils.toDate("2010-01-11");
		config.maxDate = DBUtils.toDate("2020-12-21");
		config.numberOfContractsRecords = 10;
		config.expirationDateRate =5;
		config.noExpirationDateRate = 11;
		config.duplicatePhoneNumberRatio = 2;

		// Generaterで10レコード作成する
		new CreateTable().execute(config);
		truncateTable("contracts");
		TestDataGenerator generator = new TestDataGenerator(config);
		generator.generateContracts();
		List<Contract> expectedList = getContracts();

		// MasterInsertAppで5レコード生成し、Generatorで生成したレコードと一致することを確認
		truncateTable("contracts");
		Random random = new Random(config.randomSeed);
		ContractKeyHolder contractKeyHolder = new ContractKeyHolder(config);
		MasterInsertApp app = new MasterInsertApp(contractKeyHolder, config, random);
		app.exec();
		app.exec();
		app.exec();
		app.exec();
		app.exec();
		app.getConnection().commit();
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
		app.getConnection().commit();
		list = getContracts();
		assertEquals(10, list.size());
		assertEquals(expectedList, list);

	}


	protected List<Contract> getContracts() throws SQLException {
		List<Contract> list = new ArrayList<Contract>();
		String sql = "select phone_number, start_date, end_date, charge_rule from contracts";
		ResultSet rs = stmt.executeQuery(sql);
		while (rs.next()) {
			Contract c = new Contract();
			c.phoneNumber = rs.getString(1);
			c.startDate = rs.getDate(2);
			c.endDate = rs.getDate(3);
			c.rule = rs.getString(4);
			list.add(c);
		}
		return list;
	}

}
