package com.tsurugidb.benchmark.phonebill.db;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;
import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.PhoneBillDbManagerIceaxe;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.dao.BillingDaoIceaxe;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.dao.ContractDaoIceaxe;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.dao.DdlIceaxe;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.dao.HistoryDaoIceaxe;
import com.tsurugidb.benchmark.phonebill.db.jdbc.PhoneBillDbManagerJdbc;
import com.tsurugidb.benchmark.phonebill.db.jdbc.dao.BillingDaoJdbc;
import com.tsurugidb.benchmark.phonebill.db.jdbc.dao.ContractDaoJdbc;
import com.tsurugidb.benchmark.phonebill.db.jdbc.dao.ContractDaoJdbcNoBatchUpdate;
import com.tsurugidb.benchmark.phonebill.db.jdbc.dao.HistoryDaoJdbc;
import com.tsurugidb.benchmark.phonebill.db.jdbc.dao.HistoryDaoJdbcNoBatchUpdate;
import com.tsurugidb.benchmark.phonebill.db.oracle.PhoneBillDbManagerOracle;
import com.tsurugidb.benchmark.phonebill.db.oracle.dao.DdlOracle;
import com.tsurugidb.benchmark.phonebill.db.postgresql.PhoneBillDbManagerPostgresql;
import com.tsurugidb.benchmark.phonebill.db.postgresql.PhoneBillDbManagerPostgresqlNoBatchUpdate;
import com.tsurugidb.benchmark.phonebill.db.postgresql.dao.DdlPostgresql;
import com.tsurugidb.benchmark.phonebill.db.postgresql.dao.DdlPostgresqlNoBatchUpdate;

class PhoneBillDbManagerTest extends AbstractPhoneBillDbManagerTest {
	@Test
	void testCommitAndRollback() throws Exception {

		Config config = Config.getConfig();
		createTestData(config);
		try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {

			List<History> before = getHistories();
			List<History> after = new ArrayList<>(before);

			// 1レコード更新
			History h = before.get(0).clone();
			h.setCharge(h.getCharge() == null ? 300 : h.getCharge() + 300);
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
			h.setCharge(h.getCharge() + 500);
			dao.update(h);
			after.set(0, h);

			// コミット前は別コネクションに更新が反映されない
			assertIterableEquals(before, getHistories());
			assertIterableEquals(after, getHistories(manager));

			// ロールバック後にもとに戻っている
			manager.rollback();
			assertIterableEquals(before, getHistories());
			assertIterableEquals(before, getHistories(manager));

		}

		// コミットに失敗するケース
		try (PhoneBillDbManagerJdbc manager = (PhoneBillDbManagerJdbc) PhoneBillDbManager.createPhoneBillDbManager(config)) {
			manager.getConnection().close();
			// コネクションがクローズされているため、コミットに失敗する
			RuntimeException e = assertThrows(RuntimeException.class, () ->manager.commit());
			assertTrue(e.getCause() instanceof SQLException);

		}

		// ロールバックに失敗するケース
		try (PhoneBillDbManagerJdbc manager = (PhoneBillDbManagerJdbc) PhoneBillDbManager.createPhoneBillDbManager(config)) {
			manager.getConnection().close();
			// コネクションがクローズされているため、ロールバックに失敗する
			RuntimeException e = assertThrows(RuntimeException.class, () ->manager.rollback());
			assertTrue(e.getCause() instanceof SQLException);

		}

	}

	/**
	 * getDdl, getXXXDaoのテストのためのメソッド。
	 * 想定通りの型のクラスが取得できることとと、2回目の呼び出しで最初と同じ
	 * オブジェクトが取得できることを確認する。
	 *
	 * @param <T>
	 * @param clazz
	 * @param supplier
	 * @return
	 */
	private <T> T doTestGet(Class<?> clazz, Supplier<T> supplier) {
		T t = supplier.get();
		assertEquals(clazz, t.getClass());
		assertEquals(t, supplier.get());
		return t;
	}

	@Test
	void testGetDdl() {
		doTestGet(DdlPostgresql.class, ()->getManagerPostgresql().getDdl());
		doTestGet(DdlOracle.class, ()->getManagerOracle().getDdl());
		doTestGet(DdlPostgresqlNoBatchUpdate.class, ()->getManagerPostgresqlNoBatchupdate().getDdl());
		doTestGet(DdlIceaxe.class, ()->getManagerIceaxe().getDdl());
	}

	@Test
	void testGetContractDao() {
		doTestGet(ContractDaoJdbc.class, ()->getManagerPostgresql().getContractDao());
		doTestGet(ContractDaoJdbcNoBatchUpdate.class, ()->getManagerPostgresqlNoBatchupdate().getContractDao());
		doTestGet(ContractDaoJdbc.class, ()->getManagerOracle().getContractDao());
		doTestGet(ContractDaoIceaxe.class, ()->getManagerIceaxe().getContractDao());
	}

	@Test
	void testGetHistoryDao() {
		doTestGet(HistoryDaoJdbc.class, ()->getManagerPostgresql().getHistoryDao());
		doTestGet(HistoryDaoJdbcNoBatchUpdate.class, ()->getManagerPostgresqlNoBatchupdate().getHistoryDao());
		doTestGet(HistoryDaoJdbc.class, ()->getManagerOracle().getHistoryDao());
		doTestGet(HistoryDaoIceaxe.class, ()->getManagerIceaxe().getHistoryDao());
	}

	@Test
	void testGetBillingDao() {
		doTestGet(BillingDaoJdbc.class, ()->getManagerPostgresql().getBillingDao());
		doTestGet(BillingDaoJdbc.class, ()->getManagerPostgresqlNoBatchupdate().getBillingDao());
		doTestGet(BillingDaoJdbc.class, ()->getManagerOracle().getBillingDao());
		doTestGet(BillingDaoIceaxe.class, ()->getManagerIceaxe().getBillingDao());
	}

	@Test
	void testCreatePhoneBillDbManager() throws IOException {
		assertEquals(PhoneBillDbManagerOracle.class, PhoneBillDbManager.createPhoneBillDbManager(getConfigOracle()).getClass());
		assertEquals(PhoneBillDbManagerPostgresql.class,
				PhoneBillDbManager.createPhoneBillDbManager(getConfigPostgresql()).getClass());
		assertEquals(PhoneBillDbManagerPostgresqlNoBatchUpdate.class,
				PhoneBillDbManager.createPhoneBillDbManager(getConfigPostgresqlNoBatchUpdate()).getClass());
		assertEquals(PhoneBillDbManagerIceaxe.class,
				PhoneBillDbManager.createPhoneBillDbManager(getConfigIceaxe()).getClass());
	}
}
