package com.example.nedo.app;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;

import com.example.nedo.AbstractDbTestCase;

class CreateTableTest extends AbstractDbTestCase {

	@Test
	void test() throws SQLException, IOException {
		CreateTable createTable = new CreateTable();
		createTable.dropTables(stmt);
		// テーブルが存在しないことを確認
		assertFalse(exists("billing"));
		assertFalse(exists("contracts"));
		assertFalse(exists("history"));

		// テーブルが作成されることを確認
		createTable.createBillingTable(stmt);
		assertTrue(exists("billing"));
		createTable.createContractsTable(stmt);
		assertTrue(exists("contracts"));
		createTable.createHistoryTable(stmt);
		assertTrue(exists("history"));
	}

	@Test
	void testExecute() throws Exception {
		CreateTable createTable = new CreateTable();
		createTable.execute(Config.getConfig());

		// テーブルが作成されることを確認
		assertTrue(exists("billing"));
		assertTrue(exists("contracts"));
		assertTrue(exists("history"));
	}



	/**
	 * 指定した名称のテーブルが存在するかチェックする
	 *
	 * @param tablename
	 * @return 存在するときtrue
	 * @throws SQLException
	 */
	private boolean exists(String tablename) throws SQLException {
		String sql = "SELECT count(*) from pg_class WHERE relkind = 'r' AND relname = '"+tablename+"'";
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		int c = 0;
		if (rs.next()) {
			c = rs.getInt(1);
		}
		return c == 1;
	}

}
