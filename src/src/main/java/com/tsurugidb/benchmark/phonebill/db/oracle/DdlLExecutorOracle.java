package com.tsurugidb.benchmark.phonebill.db.oracle;

import java.sql.SQLException;
import java.sql.Statement;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.jdbc.DdlExectorJdbc;
import com.tsurugidb.benchmark.phonebill.db.jdbc.PhoneBillDbManagerJdbc;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class DdlLExecutorOracle extends DdlExectorJdbc {
	private Config config;
	private PhoneBillDbManagerJdbc manager;

	public DdlLExecutorOracle(PhoneBillDbManagerJdbc manager, Config config) {
		super(manager);
		this.config = config;
		this.manager = manager;
	}

	public void createHistoryTable() {
		String create_table = "create table history ("
				+ "caller_phone_number varchar(15) not null," 		// 発信者電話番号
				+ "recipient_phone_number varchar(15) not null," 	// 受信者電話番号
				+ "payment_categorty char(1) not null," 			// 料金区分
				+ "start_time timestamp not null,"			 		// 通話開始時刻
				+ "time_secs integer not null," 					// 通話時間(秒)
				+ "charge integer," 								// 料金
				+ "df integer not null" 							// 論理削除フラグ
				+ ")";
		if (config.oracleInitran != 0) {
			create_table = create_table + "initrans " + config.oracleInitran;
		}
		try (Statement stmt = manager.getConnection().createStatement()){
			stmt.execute(create_table);
			manager.commit();
		} catch (SQLException e) {
			manager.rollback();
			throw new java.lang.RuntimeException(e);
		}
	}

	public void dropTables() {
		try (Statement stmt = manager.getConnection().createStatement()) {
			dropTableOracle(stmt, "history");
			dropTableOracle(stmt, "contracts");
			dropTableOracle(stmt, "billing");
		} catch (SQLException e) {
			manager.rollback();
			throw new java.lang.RuntimeException(e);
		}
		manager.commit();
	}

	public void prepareLoadData() {
		try (Statement stmt = manager.getConnection().createStatement()) {
			long startTime = System.currentTimeMillis();
			stmt.executeUpdate("truncate table history");
			stmt.executeUpdate("truncate table contracts");
			manager.commit();

			dropPrimaryKey("history", "history_pkey", stmt);
			dropPrimaryKey("contracts", "contracts_pkey", stmt);
			dropIndex("idx_df", stmt);
			dropIndex("idx_st", stmt);
			dropIndex("idx_rp", stmt);
			manager.commit();
			long elapsedTime = System.currentTimeMillis() - startTime;
			String format = "Truncate teable and drop indexies in %,.3f sec ";
			LOG.info(String.format(format, elapsedTime / 1000d));
		} catch (SQLException e) {
			manager.rollback();
			throw new java.lang.RuntimeException(e);
		}
	}


	@SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
	public void createIndexes() {
		String option = config.oracleCreateIndexOption;

		long startTime = System.currentTimeMillis();
		try (Statement stmt = manager.getConnection().createStatement()) {

			String create_index_df = "create index idx_df on history(df) " + option;
			execSql(stmt, create_index_df);
			String create_index_st = "create index idx_st on history(start_time) " + option;
			execSql(stmt, create_index_st);
			String create_index_rp = "create index idx_rp on history(recipient_phone_number, start_time) " + option;
			execSql(stmt, create_index_rp);
			String addPrimaryKeyToHistory = "alter table history add constraint history_pkey "
					+ "primary key (caller_phone_number, start_time) " + option;
			execSql(stmt, addPrimaryKeyToHistory);
			String addPrimaryKeyToContracts = "alter table contracts add constraint contracts_pkey "
					+ "primary key (phone_number, start_date) " + option;
			execSql(stmt, addPrimaryKeyToContracts);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		manager.commit();

		long elapsedTime = System.currentTimeMillis() - startTime;
		String format = "Create indexies in %,.3f sec ";
		LOG.info(String.format(format, elapsedTime / 1000d));
	}

	/**
	 * 指定のテーブルからPrimaryKeyを削除する
	 *
	 * @param table
	 * @param pk
	 * @param stmt
	 * @param config
	 * @throws SQLException
	 */
	@SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
	private void dropPrimaryKey(String table, String pk, Statement stmt) throws SQLException {
		try {
			stmt.execute("alter table " + table + " drop primary key");
		} catch (SQLException e) {
			if (e.getErrorCode() != 2441) { // ORA-02441: 存在しない主キーを削除することはできませんは無視する
				throw e;
			}
		}
	}

	/**
	 * 指定のテーブルから指定のIndexを削除する
	 *
	 * @param index
	 * @param stmt
	 * @param config
	 * @throws SQLException
	 */
	@SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
	private void dropIndex(String index, Statement stmt) throws SQLException {
		try {
			stmt.execute("drop index " + index);
		} catch (SQLException e) {
			if (e.getErrorCode() != 1418) { // ORA-01418: 指定した索引は存在しませんは無視する
				throw e;
			}
		}
	}

	@SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
	protected
	void dropTableOracle(Statement stmt, String table) throws SQLException {
		try {
			stmt.execute("drop table "+ table);
		} catch (SQLException e) {
			if (e.getErrorCode() != 942) { // 「ORA-00942 表またはビューが存在しません」は無視する
				throw e;
			}
		}
	}

	public void updateStatistics() {
		long startTime = System.currentTimeMillis();
		try (Statement stmt = manager.getConnection().createStatement()) {
			stmt.executeUpdate("{call DBMS_STATS.GATHER_SCHEMA_STATS(ownname => '" + config.user
					+ "', cascade => TRUE, no_invalidate => TRUE)}");
		} catch (SQLException e) {
			manager.rollback();
			throw new RuntimeException(e);
		}
		manager.commit();
		long elapsedTime = System.currentTimeMillis() - startTime;
		String format = "Update statistic in %,.3f sec ";
		LOG.info(String.format(format, elapsedTime / 1000d));
	}
}
