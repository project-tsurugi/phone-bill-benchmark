package com.tsurugidb.benchmark.phonebill.db.jdbc.dao;

import java.sql.SQLException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.db.jdbc.PhoneBillDbManagerJdbc;

class ContractDaoJdbcNoBatchUpdateTest extends ContractDaoJdbcTest {

	@Test
	void testBatchInsert() throws SQLException {
		ContractDaoJdbcNoBatchUpdate dao = new ContractDaoJdbcNoBatchUpdate((PhoneBillDbManagerJdbc) getManager());
		testBatchInsertSub(dao);
	}

}
