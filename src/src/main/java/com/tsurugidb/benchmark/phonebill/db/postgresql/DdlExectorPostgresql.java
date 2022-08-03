package com.tsurugidb.benchmark.phonebill.db.postgresql;

import java.sql.SQLException;
import java.sql.Statement;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.jdbc.DdlExectorJdbc;
import com.tsurugidb.benchmark.phonebill.db.jdbc.PhoneBillDbManagerJdbc;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class DdlExectorPostgresql extends DdlExectorJdbc {
	private Config config;
	private PhoneBillDbManagerJdbc manager;

	public DdlExectorPostgresql(PhoneBillDbManagerJdbc manager, Config config) {
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
		try (Statement stmt = manager.getConnection().createStatement()){
			stmt.execute(create_table);
			manager.commit();
		} catch (SQLException e) {
			manager.rollback();
			throw new RuntimeException(e);
		}
	}

	public void dropTables() {
		try (Statement stmt = manager.getConnection().createStatement()) {
			// 通話履歴テーブル
			dropTable(stmt, "history");
			dropTable(stmt, "contracts");
			dropTable(stmt, "billing");
		} catch (SQLException e) {
			manager.rollback();
			throw new RuntimeException(e);
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
			throw new RuntimeException(e);
		}
	}

	@SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
	public void createIndexes() {
		long startTime = System.currentTimeMillis();
		try (Statement stmt = manager.getConnection().createStatement()) {

			String create_index_df = "create index idx_df on history(df)";
			execSql(stmt, create_index_df);
			String create_index_st = "create index idx_st on history(start_time)";
			execSql(stmt, create_index_st);
			String create_index_rp = "create index idx_rp on history(recipient_phone_number, start_time)";
			execSql(stmt, create_index_rp);
			String addPrimaryKeyToHistory = "alter table history add constraint history_pkey "
					+ "primary key (caller_phone_number, start_time)";
			execSql(stmt, addPrimaryKeyToHistory);
			String addPrimaryKeyToContracts = "alter table contracts add constraint contracts_pkey "
					+ "primary key (phone_number, start_date)";
			execSql(stmt, addPrimaryKeyToContracts);
		} catch (SQLException e) {
			manager.rollback();
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
		stmt.execute("alter table " + table + " drop constraint if exists " + pk);
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
		stmt.execute("drop index if exists " + index);
	}

	@SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
	public void updateStatistics() {
		long startTime = System.currentTimeMillis();
		try (Statement stmt = manager.getConnection().createStatement()) {
			stmt.executeUpdate("analyze history");
			stmt.executeUpdate("analyze contracts");
		} catch (SQLException e) {
			manager.rollback();
			throw new RuntimeException(e);
		}
		manager.commit();
		long elapsedTime = System.currentTimeMillis() - startTime;
		String format = "Update statistic in %,.3f sec ";
		LOG.info(String.format(format, elapsedTime / 1000d));
	}

	@SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
	private void dropTable(Statement stmt, String table) throws SQLException {
		stmt.execute("drop table if exists " + table);
	}
}
