package com.example.nedo.online;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.sql.SQLException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.example.nedo.app.Config;
import com.example.nedo.app.CreateTable;
import com.example.nedo.db.Contract;
import com.example.nedo.db.DBUtils;
import com.example.nedo.testdata.TestDataGenerator;

class ContractHolderTest {
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
		ContractHolder contractHolder = new ContractHolder(Config.getConfig());
		assertEquals(5, contractHolder.size());
	}

	@Test
	void testGet() throws SQLException, IOException {
		ContractHolder contractHolder = new ContractHolder(Config.getConfig());
		assertEquals("00000000000", contractHolder.get(0).phoneNumber);
		assertEquals("00000000001", contractHolder.get(1).phoneNumber);
		assertEquals("00000000002", contractHolder.get(2).phoneNumber);
		assertEquals("00000000003", contractHolder.get(3).phoneNumber);
		assertEquals("00000000004", contractHolder.get(4).phoneNumber);

		assertEquals(DBUtils.toDate("2017-03-17"), contractHolder.get(0).startDate);
		assertEquals(DBUtils.toDate("2018-08-23"), contractHolder.get(1).startDate);
		assertEquals(DBUtils.toDate("2016-09-07"), contractHolder.get(2).startDate);
		assertEquals(DBUtils.toDate("2020-07-06"), contractHolder.get(3).startDate);
		assertEquals(DBUtils.toDate("2010-10-17"), contractHolder.get(4).startDate);

		assertNull(contractHolder.get(0).endDate);
		assertNull(contractHolder.get(1).endDate);
		assertNull(contractHolder.get(2).endDate);
		assertNull(contractHolder.get(3).endDate);
		assertNull(contractHolder.get(4).endDate);

		assertEquals("sample", contractHolder.get(0).rule);
		assertEquals("sample", contractHolder.get(1).rule);
		assertEquals("sample", contractHolder.get(2).rule);
		assertEquals("sample", contractHolder.get(3).rule);
		assertEquals("sample", contractHolder.get(4).rule);
	}

	@Test
	void testAdd() throws SQLException, IOException {
		ContractHolder contractHolder = new ContractHolder(Config.getConfig());
		Contract c = new Contract();
		c.phoneNumber = "phone-number";
		c.startDate = DBUtils.toDate("2020-05-05");
		c.endDate = DBUtils.toDate("2022-05-09");
		c.rule = "TEST RULE";
		contractHolder.add(c);
		assertEquals(6, contractHolder.size());
		assertEquals("phone-number", contractHolder.get(5).phoneNumber);
		assertEquals(DBUtils.toDate("2020-05-05"), contractHolder.get(5).startDate);
		assertEquals(DBUtils.toDate("2022-05-09"), contractHolder.get(5).endDate);
		assertEquals("TEST RULE", contractHolder.get(5).rule);
	}

	@Test
	void testReplace() throws SQLException, IOException {
		ContractHolder contractHolder = new ContractHolder(Config.getConfig());
		Contract c0 = new Contract();
		c0.phoneNumber = "00000000000";
		c0.startDate = DBUtils.toDate("2017-03-17");
		c0.endDate = null;
		c0.rule = "replace rule 0";
		contractHolder.replace(c0);

		Contract c4 = new Contract();
		c4.phoneNumber = "00000000004";
		c4.startDate = DBUtils.toDate("2010-10-17");
		c4.endDate = DBUtils.toDate("2020-08-04");
		c4.rule = "replace rule 4";
		contractHolder.replace(c4);

		assertEquals(5, contractHolder.size());

		assertEquals("00000000000", contractHolder.get(0).phoneNumber);
		assertEquals("00000000001", contractHolder.get(1).phoneNumber);
		assertEquals("00000000002", contractHolder.get(2).phoneNumber);
		assertEquals("00000000003", contractHolder.get(3).phoneNumber);
		assertEquals("00000000004", contractHolder.get(4).phoneNumber);

		assertEquals(DBUtils.toDate("2017-03-17"), contractHolder.get(0).startDate);
		assertEquals(DBUtils.toDate("2018-08-23"), contractHolder.get(1).startDate);
		assertEquals(DBUtils.toDate("2016-09-07"), contractHolder.get(2).startDate);
		assertEquals(DBUtils.toDate("2020-07-06"), contractHolder.get(3).startDate);
		assertEquals(DBUtils.toDate("2010-10-17"), contractHolder.get(4).startDate);

		assertNull(contractHolder.get(0).endDate);
		assertNull(contractHolder.get(1).endDate);
		assertNull(contractHolder.get(2).endDate);
		assertNull(contractHolder.get(3).endDate);
		assertEquals(DBUtils.toDate("2020-08-04"), contractHolder.get(4).endDate);

		assertEquals("replace rule 0", contractHolder.get(0).rule);
		assertEquals("sample", contractHolder.get(1).rule);
		assertEquals("sample", contractHolder.get(2).rule);
		assertEquals("sample", contractHolder.get(3).rule);
		assertEquals("replace rule 4", contractHolder.get(4).rule);
	}

}
