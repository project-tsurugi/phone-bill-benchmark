package com.example.nedo.online;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.example.nedo.app.Config;
import com.example.nedo.app.CreateTable;
import com.example.nedo.db.DBUtils;
import com.example.nedo.testdata.TestDataGenerator;

class ContractKeyHolderTest {
	/**
	 * 契約テーブルにテストデータをセットする
	 *
	 * @throws IOException
	 * @throws Exception
	 */
	@BeforeEach
	void initContractTable() throws IOException, Exception {
		new CreateTable().execute(Config.getConfig());

		Config config = Config.getConfig();
		config.minDate = DBUtils.toDate("2010-01-11");
		config.maxDate = DBUtils.toDate("2020-12-21");
		config.numberOfContractsRecords = 5;
		config.expirationDateRate =5;
		config.noExpirationDateRate = 11;
		config.duplicatePhoneNumberRatio = 2;
		TestDataGenerator generator = new TestDataGenerator(config);
		generator.generateContracts();
	}


	@Test
	void testGetSize() throws SQLException, IOException {
		ContractKeyHolder contractKeyHolder = new ContractKeyHolder(Config.getConfig());
		assertEquals(5, contractKeyHolder.size());
	}

	@Test
	void testGetkey() throws SQLException, IOException {
		ContractKeyHolder contractKeyHolder = new ContractKeyHolder(Config.getConfig());
		assertEquals("00000000000", contractKeyHolder.get(0).phoneNumber);
		assertEquals("00000000001", contractKeyHolder.get(1).phoneNumber);
		assertEquals("00000000002", contractKeyHolder.get(2).phoneNumber);
		assertEquals("00000000003", contractKeyHolder.get(3).phoneNumber);
		assertEquals("00000000004", contractKeyHolder.get(4).phoneNumber);

		assertEquals(DBUtils.toDate("2017-03-17"), contractKeyHolder.get(0).startDate);
		assertEquals(DBUtils.toDate("2018-08-23"), contractKeyHolder.get(1).startDate);
		assertEquals(DBUtils.toDate("2016-09-07"), contractKeyHolder.get(2).startDate);
		assertEquals(DBUtils.toDate("2020-07-06"), contractKeyHolder.get(3).startDate);
		assertEquals(DBUtils.toDate("2010-10-17"), contractKeyHolder.get(4).startDate);
	}

	@Test
	void testAddKeyKey() throws SQLException, IOException {
		ContractKeyHolder contractKeyHolder = new ContractKeyHolder(Config.getConfig());
		contractKeyHolder.add(ContractKeyHolder.createKey("phone-number", DBUtils.toDate("2020-05-05")));
		assertEquals(6, contractKeyHolder.size());
		assertEquals("phone-number", contractKeyHolder.get(5).phoneNumber);
		assertEquals(DBUtils.toDate("2020-05-05"), contractKeyHolder.get(5).startDate);
	}

	@Test
	void testReplace() throws SQLException, IOException {
		ContractKeyHolder contractKeyHolder = new ContractKeyHolder(Config.getConfig());
		contractKeyHolder.replace(0, ContractKeyHolder.createKey("replace0", DBUtils.toDate("2020-10-10")));
		contractKeyHolder.replace(4, ContractKeyHolder.createKey("replace4", DBUtils.toDate("2020-04-04")));
		assertThrows(IllegalArgumentException.class, () -> contractKeyHolder.replace(-1, null));
		assertThrows(IllegalArgumentException.class, () -> contractKeyHolder.replace(5, null));
		assertEquals(5, contractKeyHolder.size());
		assertEquals("replace0", contractKeyHolder.get(0).phoneNumber);
		assertEquals("00000000001", contractKeyHolder.get(1).phoneNumber);
		assertEquals("00000000002", contractKeyHolder.get(2).phoneNumber);
		assertEquals("00000000003", contractKeyHolder.get(3).phoneNumber);
		assertEquals("replace4", contractKeyHolder.get(4).phoneNumber);

		assertEquals(DBUtils.toDate("2020-10-10"), contractKeyHolder.get(0).startDate);
		assertEquals(DBUtils.toDate("2018-08-23"), contractKeyHolder.get(1).startDate);
		assertEquals(DBUtils.toDate("2016-09-07"), contractKeyHolder.get(2).startDate);
		assertEquals(DBUtils.toDate("2020-07-06"), contractKeyHolder.get(3).startDate);
		assertEquals(DBUtils.toDate("2020-04-04"), contractKeyHolder.get(4).startDate);
	}

}
