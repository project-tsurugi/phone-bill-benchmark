package com.tsurugidb.benchmark.phonebill.app;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.AbstractDbTestCase;
import com.tsurugidb.benchmark.phonebill.app.Config.DbmsType;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.SessionException;
import com.tsurugidb.benchmark.phonebill.db.interfaces.DdlLExecutor;
import com.tsurugidb.benchmark.phonebill.db.jdbc.DBUtils;

class CreateTableTest extends AbstractDbTestCase {
	private static String ORACLE_CONFIG_PATH = "src/test/config/oracle.properties";

	@Test
	void test() throws SQLException, IOException, SessionException {
		Config config = Config.getConfig();
		PhoneBillDbManager manager = PhoneBillDbManager.createInstance(config);
		DdlLExecutor ddlExector = manager.getDdlLExecutor();

		ddlExector.dropTables();
		// テーブルが存在しないことを確認
		assertFalse(existsTable("billing"));
		assertFalse(existsTable("contracts"));
		assertFalse(existsTable("history"));

		// テーブルが作成されることを確認
		ddlExector.createBillingTable();
		assertTrue(existsTable("billing"));
		ddlExector.createContractsTable();
		assertTrue(existsTable("contracts"));
		ddlExector.createHistoryTable();
		assertTrue(existsTable("history"));
	}

	@Test
	void testExecute() throws Exception {
		CreateTable createTable = new CreateTable();
		createTable.execute(Config.getConfig());

		// テーブルが作成されることを確認
		assertTrue(existsTable("billing"));
		assertTrue(existsTable("contracts"));
		assertTrue(existsTable("history"));
	}


	/**
	 * prepareLoadData()とafterLoadData()のテスト
	 *
	 * @throws Exception
	 */
	@Test
	void testPrepareAndAfterLoadData() throws Exception {
		testPrepareAndAfterLoadDataSub(Config.getConfig());
	}

	/**
	 * prepareLoadData()とafterLoadData()のテスト(oracle)
	 *
	 * @throws Exception
	 */
	@Test
	@Tag("oracle")
	void testPrepareAndAfterLoadDataOracle() throws Exception {
		testPrepareAndAfterLoadDataSub(Config.getConfig(ORACLE_CONFIG_PATH));
	}



	private void testPrepareAndAfterLoadDataSub(Config config) throws Exception {
		try (Connection conn = DBUtils.getConnection(config);
				Statement stmt = conn.createStatement()) {
			conn.setAutoCommit(false);

			// テーブルを作成
			CreateTable createTable = new CreateTable();
			createTable.execute(config);

			String history = config.dbmsType == DbmsType.ORACLE_JDBC ? "HISTORY" : "history";
			String contracts = config.dbmsType == DbmsType.ORACLE_JDBC ? "CONTRACTS" : "contracts";

			Set<String> historyIndexSet = new HashSet<String>();
			historyIndexSet.add("history_pkey");
			historyIndexSet.add("idx_df");
			historyIndexSet.add("idx_st");
			historyIndexSet.add("idx_rp");

			Set<String> contractsIndexSet = new HashSet<String>();
			contractsIndexSet.add("contracts_pkey");


			// インデックスの存在を確認
			assertEquals(historyIndexSet, getIndexNameSet(conn,  history));
			assertEquals(contractsIndexSet, getIndexNameSet(conn,  contracts));

// TOD: ExecuteDDLクラスのテストケースにする
//			// インデックス削除されたことを確認
//			CreateTable.prepareLoadData(stmt, config);
//			assertEquals(Collections.EMPTY_SET, getIndexNameSet(conn,  history));
//			assertEquals(Collections.EMPTY_SET, getIndexNameSet(conn,  contracts));
//
//			// インデックスが復活したことを確認
//			CreateTable.afterLoadData(stmt, config);
//			assertEquals(historyIndexSet, getIndexNameSet(conn,  history));
//			assertEquals(contractsIndexSet, getIndexNameSet(conn,  contracts));
		}
	}

// TOD: ExecuteDDLクラスのテストケースにする
//	/**
//	 * dropIndex()とdropPrimaryKey()のテスト
//	 *
//	 * @throws Exception
//	 */
//	@Test
//	void testDropIndexAndDropPrimaryKey() throws Exception {
//		testDropIndexAndDropPrimaryKeySub(Config.getConfig());
//	}

// TOD: ExecuteDDLクラスのテストケースにする

//	/**
//	 * dropIndex()とdropPrimaryKey()のテスト(oracle)
//	 *
//	 * @throws Exception
//	 */
//	@Test
//	@Tag("oracle")
//	void testDropIndexAndDropPrimaryKeyOracle() throws Exception {
//		testDropIndexAndDropPrimaryKeySub(Config.getConfig(new String[]{ORACLE_CONFIG_PATH}));
//	}

//	void testDropIndexAndDropPrimaryKeySub(Config config) throws Exception {
//		try (Connection conn = DBUtils.getConnection(config);
//				Statement stmt = conn.createStatement()) {
//			conn.setAutoCommit(true);
//
//			// テーブルを作成
//			CreateTable createTable = new CreateTable();
//			createTable.execute(config);
//
//			String tableName = config.dbmsType == DbmsType.ORACLE_JDBC ? "HISTORY" : "history";
//
//			// インデックスの存在を確認
//			assertTrue(getIndexNameSet(conn,  tableName).contains("idx_df"));
//
//			// インデックス削除
//			CreateTable.dropIndex("idx_df", stmt, config);
//			assertFalse(getIndexNameSet(conn,  tableName).contains("idx_df"));
//
//			// 2回呼んでもエラーにならない
//			CreateTable.dropIndex("idx_df", stmt, config);
//			assertFalse(getIndexNameSet(conn,  tableName).contains("idx_df"));
//
//
//			// PKの存在を確認
//			assertTrue(getIndexNameSet(conn,  tableName).contains("history_pkey"));
//
//			// PK削除
//			CreateTable.dropPrimaryKey(tableName, "history_pkey", stmt, config);
//			assertFalse(getIndexNameSet(conn,  tableName).contains("history_pkey"));
//
//			// 2回呼んでもエラーにならない
//			CreateTable.dropPrimaryKey(tableName, "history_pkey", stmt, config);
//			assertFalse(getIndexNameSet(conn,  tableName).contains("history_pkey"));
//		}
//	}

	/**
	 * 指定のテーブルに存在するインデックスのセットを返す
	 *
	 * @param indexname
	 * @return 存在するときtrue
	 * @throws SQLException
	 */
	private Set<String> getIndexNameSet(Connection conn, String table) throws SQLException {
		Set<String> set = new HashSet<>();
		DatabaseMetaData metaData = conn.getMetaData();
		try (ResultSet rs = metaData.getIndexInfo(null, null, table, false, true)) {
			while (rs.next()) {
				String str = rs.getString("INDEX_NAME");
				if(str != null) {
					set.add(str.toLowerCase(Locale.ROOT));  // Oracleの場合インデックス名が大文字になってしまうので小文字に戻す
				}
			}
		}
		return set;
	}

	/**
	 * 指定した名称のテーブルが存在するかチェックする
	 *
	 * @param tablename
	 * @return 存在するときtrue
	 * @throws SQLException
	 */
	private boolean existsTable(String tablename) throws SQLException {
		String sql = "SELECT count(*) from pg_class WHERE relkind = 'r' AND relname = ?";
		int c = 0;
		try (PreparedStatement ps = getConn().prepareStatement(sql)) {
			ps.setString(1, tablename);
			try (ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					c = rs.getInt(1);
				}
			}
		}
		return c == 1;
	}
}
