/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.benchmark.phonebill.db.oracle.dao;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.dao.Ddl;
import com.tsurugidb.benchmark.phonebill.db.jdbc.PhoneBillDbManagerJdbc;

class DdlOracleTest {
	private static String ORACLE_CONFIG_PATH = "src/test/config/oracle.properties";

	private static PhoneBillDbManager manager;
	private static Connection conn;

	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		manager = PhoneBillDbManager.createPhoneBillDbManager(Config.getConfig(ORACLE_CONFIG_PATH));
		conn = ((PhoneBillDbManagerJdbc)manager).getConnection();
		conn.setAutoCommit(false);
	}

	@AfterAll
	static void tearDownAfterClass() throws Exception {
		conn.close();
		manager.close();
	}

	@Test
	@Tag("oracle")
	final void testTableExists() {
		Ddl ddl = manager.getDdl();
		ddl.dropTables();
		assertFalse(ddl.tableExists("history"));
		assertFalse(ddl.tableExists("contracts"));
		assertFalse(ddl.tableExists("billing"));
		assertFalse(ddl.tableExists("BILLING"));
		assertFalse(ddl.tableExists("biLLing"));

		ddl.createBillingTable();
		assertFalse(ddl.tableExists("history"));
		assertFalse(ddl.tableExists("contracts"));
		assertTrue(ddl.tableExists("billing"));
		assertTrue(ddl.tableExists("BILLING"));
		assertTrue(ddl.tableExists("biLLing"));

		ddl.createContractsTable();
		assertFalse(ddl.tableExists("history"));
		assertTrue(ddl.tableExists("contracts"));
		assertTrue(ddl.tableExists("billing"));
		assertTrue(ddl.tableExists("BILLING"));
		assertTrue(ddl.tableExists("biLLing"));

		ddl.createHistoryTable();
		assertTrue(ddl.tableExists("history"));
		assertTrue(ddl.tableExists("contracts"));
		assertTrue(ddl.tableExists("billing"));
		assertTrue(ddl.tableExists("BILLING"));
		assertTrue(ddl.tableExists("biLLing"));
	}

}
