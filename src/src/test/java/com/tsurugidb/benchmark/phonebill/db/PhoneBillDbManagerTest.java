package com.tsurugidb.benchmark.phonebill.db;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.Config.DbmsType;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;
import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.benchmark.phonebill.db.jdbc.PhoneBillDbManagerJdbc;
import com.tsurugidb.benchmark.phonebill.db.jdbc.dao.BillingDaoJdbc;
import com.tsurugidb.benchmark.phonebill.db.jdbc.dao.ContractDaoJdbc;
import com.tsurugidb.benchmark.phonebill.db.jdbc.dao.HistoryDaoJdbc;
import com.tsurugidb.benchmark.phonebill.db.oracle.PhoneBillDbManagerOracle;
import com.tsurugidb.benchmark.phonebill.db.oracle.dao.DdlOracle;
import com.tsurugidb.benchmark.phonebill.db.postgresql.PhoneBillDbManagerPostgresql;
import com.tsurugidb.benchmark.phonebill.db.postgresql.dao.DdlPostgresql;

class PhoneBillDbManagerTest extends AbstractPhoneBillDbManagerTest {
	int commitCount = 0;
	int rollbackCount = 0;

	@Test
	void testCommitAndRollback() throws Exception {

		Config config = Config.getConfig();
		createTestData(config);
		try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {

			List<History> before = getHistories();
			List<History> after = new ArrayList<>(before);

			// 1レコード更新
			History h = before.get(0).clone();
			h.charge = h.charge == null ? 300 : h.charge + 300;
			after.set(0, h);
			HistoryDao dao = manager.getHistoryDao();
			dao.update(h);

			// コミット前は別コネクションに更新が反映されない
			assertIterableEquals(before, getHistories());
			assertIterableEquals(after, getHistories(manager));

			// コミット後に別コネクションでも更新が反映されている
			manager.commit();
			assertIterableEquals(after, getHistories());
			assertIterableEquals(after, getHistories(manager));

			// もう一度更新
			before = after;
			after = new ArrayList<>(before);

			assertEquals(before, after);
			h = h.clone();
			h.charge += 500;
			dao.update(h);
			after.set(0, h);

			// コミット前は別コネクションに更新が反映されない
			assertIterableEquals(before, getHistories());
			assertIterableEquals(after, getHistories(manager));

			// ロールバック後にもとに戻っている
			manager.rollback();
			assertIterableEquals(before, getHistories());
			assertIterableEquals(before, getHistories(manager));

			// commit(Runnable)のテスト
			commitCount =0;
			manager.commit(()->{commitCount++;});
			assertEquals(1, commitCount);
			manager.commit(()->{commitCount++;});
			assertEquals(2, commitCount);

			// rollback(Runnable)のテスト
			rollbackCount = 0;
			manager.rollback(()->{rollbackCount++;});
			assertEquals(1, rollbackCount);
			manager.rollback(()->{rollbackCount++;});
			assertEquals(2, rollbackCount);
		}

		// コミットに失敗するケース
		try (PhoneBillDbManagerJdbc manager = (PhoneBillDbManagerJdbc) PhoneBillDbManager.createPhoneBillDbManager(config)) {
			manager.getConnection().close();
			// コネクションがクローズされているため、コミットに失敗する
			RuntimeException e = assertThrows(RuntimeException.class, () ->manager.commit());
			assertTrue(e.getCause() instanceof SQLException);

			// Listenerが呼び出されないことの確認
			commitCount = 3;
			assertThrows(RuntimeException.class, () ->manager.commit(()->{commitCount++;}));
			assertEquals(3, commitCount);
		}

		// ロールバックに失敗するケース
		try (PhoneBillDbManagerJdbc manager = (PhoneBillDbManagerJdbc) PhoneBillDbManager.createPhoneBillDbManager(config)) {
			manager.getConnection().close();
			// コネクションがクローズされているため、ロールバックに失敗する
			RuntimeException e = assertThrows(RuntimeException.class, () ->manager.rollback());
			assertTrue(e.getCause() instanceof SQLException);

			// Listenerが呼び出されないことの確認
			rollbackCount = 4;
			assertThrows(RuntimeException.class, () ->manager.rollback(()->{rollbackCount++;}));
			assertEquals(4, rollbackCount);
		}

	}


	@Test
	void testGetDdl() {
		assertEquals(DdlOracle.class, managerOracle.getDdl().getClass());
		assertEquals(DdlPostgresql.class, managerPostgresql.getDdl().getClass());
		assertEquals(DdlOracle.class, managerOracle.getDdl().getClass());
		assertEquals(DdlPostgresql.class, managerPostgresql.getDdl().getClass());
	}

	@Test
	void testGetContractDao() {
		assertEquals(ContractDaoJdbc.class, managerOracle.getContractDao().getClass());
		assertEquals(ContractDaoJdbc.class, managerPostgresql.getContractDao().getClass());
		assertEquals(ContractDaoJdbc.class, managerOracle.getContractDao().getClass());
		assertEquals(ContractDaoJdbc.class, managerPostgresql.getContractDao().getClass());
	}

	@Test
	void testGetHistoryDao() {
		assertEquals(HistoryDaoJdbc.class, managerOracle.getHistoryDao().getClass());
		assertEquals(HistoryDaoJdbc.class, managerPostgresql.getHistoryDao().getClass());
		assertEquals(HistoryDaoJdbc.class, managerOracle.getHistoryDao().getClass());
		assertEquals(HistoryDaoJdbc.class, managerPostgresql.getHistoryDao().getClass());
	}

	@Test
	void testGetBillingDao() {
		assertEquals(BillingDaoJdbc.class, managerOracle.getBillingDao().getClass());
		assertEquals(BillingDaoJdbc.class, managerPostgresql.getBillingDao().getClass());
		assertEquals(BillingDaoJdbc.class, managerOracle.getBillingDao().getClass());
		assertEquals(BillingDaoJdbc.class, managerPostgresql.getBillingDao().getClass());
	}


	@Test
	void testCreatePhoneBillDbManager() throws IOException {
		Config config = Config.getConfig();

		config.dbmsType = DbmsType.ORACLE_JDBC;
		assertEquals(PhoneBillDbManagerOracle.class , PhoneBillDbManager.createPhoneBillDbManager(config).getClass());


		config.dbmsType = DbmsType.POSTGRE_SQL_JDBC;
		assertEquals(PhoneBillDbManagerPostgresql.class , PhoneBillDbManager.createPhoneBillDbManager(config).getClass());


		config.dbmsType = DbmsType.OTHER;
		UnsupportedOperationException e =
		assertThrows(UnsupportedOperationException.class,
				() -> PhoneBillDbManager.createPhoneBillDbManager(config).getClass());
		assertEquals("unsupported dbms type: OTHER", e.getMessage());
	}

}
