package com.tsurugidb.benchmark.phonebill.db.jdbc.dao;

import java.sql.SQLException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.db.jdbc.PhoneBillDbManagerJdbc;

class HistoryDaoJdbcNoBatchUpdateTest extends HistoryDaoJdbcTest {

	@Test
	final void testBatchInsert()  throws SQLException  {
		HistoryDaoJdbcNoBatchUpdate dao = new HistoryDaoJdbcNoBatchUpdate((PhoneBillDbManagerJdbc) getManager());
		testBatchInsertSub(dao);
	}

	@Test
	final void testBatchUpdate() throws SQLException {
		HistoryDaoJdbcNoBatchUpdate dao = new HistoryDaoJdbcNoBatchUpdate((PhoneBillDbManagerJdbc) getManager());
		testBatchInsertSub(dao);
	}
}
