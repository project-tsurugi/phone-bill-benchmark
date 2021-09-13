package com.example.nedo.app;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.app.Config.Dbms;
import com.example.nedo.db.DBUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class CreateTable implements ExecutableCommand{
    private static final Logger LOG = LoggerFactory.getLogger(CreateTable.class);

	public static void main(String[] args) throws Exception {
		Config config = Config.getConfig(args);
		CreateTable createTable = new CreateTable();
		createTable.execute(config);
	}

	@Override
	public void execute(Config config) throws Exception {
//		boolean isOracle = config.dbms == Dbms.ORACLE;
		try (Connection conn = DBUtils.getConnection(config);
				Statement stmt = conn.createStatement()) {
			dropTables(stmt, config);
			createHistoryTable(stmt, config);
			createContractsTable(stmt);
			createBillingTable(stmt);
			afterLoadData(stmt, config);
		}
	}

	void createHistoryTable(Statement stmt, Config config) throws SQLException {
		String create_table = "create table history ("
				+ "caller_phone_number varchar(15) not null," 		// 発信者電話番号
				+ "recipient_phone_number varchar(15) not null," 	// 受信者電話番号
				+ "payment_categorty char(1) not null," 			// 料金区分
				+ "start_time timestamp not null,"			 		// 通話開始時刻
				+ "time_secs integer not null," 					// 通話時間(秒)
				+ "charge integer," 								// 料金
				+ "df integer not null" 							// 論理削除フラグ
				+ ")";
		if (config.dbms == Dbms.ORACLE && config.oracleInitran != 0) {
			create_table = create_table + "initrans " + config.oracleInitran;
		}
		stmt.execute(create_table);
	}

	void createContractsTable(Statement stmt) throws SQLException {
		String create_table = "create table contracts ("
				+ "phone_number varchar(15) not null," 		// 電話番号
				+ "start_date date not null," 				// 契約開始日
				+ "end_date date,"							// 契約終了日
				+ "charge_rule varchar(255) not null"		// 料金計算ルール
				+ ")";
		stmt.execute(create_table);
	}

	void createBillingTable(Statement stmt) throws SQLException {
		String create_table = "create table billing ("
				+ "phone_number varchar(15) not null," 					// 電話番号
				+ "target_month date not null," 						// 対象年月
				+ "basic_charge integer not null," 						// 基本料金
				+ "metered_charge integer not null,"					// 従量料金
				+ "billing_amount integer not null,"					// 請求金額
				+ "batch_exec_id varchar(36) not null,"					// バッチ実行ID
				+ "constraint  billing_pkey primary key(target_month, phone_number, batch_exec_id)"
				+ ")";
		stmt.execute(create_table);
	}


	void dropTables(Statement stmt, Config config) throws SQLException {
		// 通話履歴テーブル
		if (config.dbms == Dbms.ORACLE) {
			dropTableOracle(stmt, "history");
			dropTableOracle(stmt, "contracts");
			dropTableOracle(stmt, "billing");
		} else {
			dropTable(stmt, "history");
			dropTable(stmt, "contracts");
			dropTable(stmt, "billing");
		}
		stmt.getConnection().commit();
	}

	@SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
	void dropTable(Statement stmt, String table) throws SQLException {
		stmt.execute("drop table if exists "+ table);
	}

	@SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
	void dropTableOracle(Statement stmt, String table) throws SQLException {
		try {
			stmt.execute("drop table "+ table);
		} catch (SQLException e) {
			if (e.getErrorCode() != 942) { // 「ORA-00942 表またはビューが存在しません」は無視する
				throw e;
			}
		}
	}

	/**
	 * テストデータのロード前の処理.
	 * <br>
	 * プライマリーキー、インデックスとテーブルデータを削除する
	 *
	 * @param stmt
	 * @param config
	 * @throws SQLException
	 */
	static public void prepareLoadData(Statement stmt, Config config) throws SQLException {
		long startTime = System.currentTimeMillis();
		stmt.executeUpdate("truncate table history");
		stmt.executeUpdate("truncate table contracts");
		stmt.getConnection().commit();

		dropPrimaryKey("history", "history_pkey", stmt, config);
		dropPrimaryKey("contracts", "contracts_pkey", stmt, config);
		dropIndex("idx_df", stmt, config);
		dropIndex("idx_st", stmt, config);
		dropIndex("idx_rp", stmt, config);
		stmt.getConnection().commit();
		long elapsedTime = System.currentTimeMillis() - startTime;
		String format = "Truncate teable and drop indexies in %,.3f sec ";
		LOG.info(String.format(format, elapsedTime / 1000d));
	}

	/**
	 * テストデータのロード後の処理.
	 * <br>
	 * プライマリーキー、インデックスを作成し、統計情報を更新する
	 *
	 * @param stmt
	 * @param config
	 * @throws SQLException
	 */
	static public void afterLoadData(Statement stmt, Config config) throws SQLException {
		createIndexes(stmt, config);
		updateStatistics(config);
	}

	/**
	 * インデックスを生成する
	 *
	 * @param stmt
	 * @param config
	 * @throws SQLException
	 */
	@SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
	static void createIndexes(Statement stmt, Config config) throws SQLException {
		String option = config.dbms == Dbms.ORACLE ? config.oracleCreateIndexOption : "";

		long startTime = System.currentTimeMillis();

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
				+ "primary key (phone_number, start_date) "  + option;
		execSql(stmt, addPrimaryKeyToContracts);
		stmt.getConnection().commit();

		long elapsedTime = System.currentTimeMillis() - startTime;
		String format = "Create indexies in %,.3f sec ";
		LOG.info(String.format(format, elapsedTime / 1000d));
	}

	/*
	 * 指定のSQLを実行前後にログを入れて実行する
	 */
	static void execSql(Statement stmt, String sql) throws SQLException {
		long startTime = System.currentTimeMillis();
		LOG.info("start exec sql:" + sql);
		stmt.execute(sql);
		long elapsedTime = System.currentTimeMillis() - startTime;
		String format = "end exec sql: " + sql + " in %,.3f sec ";
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
	static void dropPrimaryKey(String table, String pk, Statement stmt, Config config) throws SQLException {
		if (config.dbms == Dbms.ORACLE) {
			try {
				stmt.execute("alter table " + table + " drop primary key");
			} catch (SQLException e) {
				if (e.getErrorCode() != 2441) { // ORA-02441: 存在しない主キーを削除することはできませんは無視する
					throw e;
				}
			}
		} else {
			stmt.execute("alter table " + table + " drop constraint if exists " + pk);
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
	static void dropIndex(String index, Statement stmt, Config config) throws SQLException {
		if (config.dbms == Dbms.ORACLE) {
			try {
				stmt.execute("drop index " + index);
			} catch (SQLException e) {
				if (e.getErrorCode() != 1418) { // ORA-01418: 指定した索引は存在しませんは無視する
					throw e;
				}
			}
		} else {
			stmt.execute("drop index if exists " + index);
		}
	}

	/**
	 * 統計情報を更新する
	 * @throws SQLException
	 */
	@SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
	public static void updateStatistics(Config config) throws SQLException {
		// DBMSの統計情報を更新する
		long startTime = System.currentTimeMillis();
		try (Connection conn = DBUtils.getConnection(config);
				Statement stmt = conn.createStatement()) {
			switch (config.dbms) {
			case ORACLE:
				stmt.executeUpdate("{call DBMS_STATS.GATHER_SCHEMA_STATS(ownname => '" + config.user
						+ "', cascade => TRUE, no_invalidate => TRUE)}");
				break;
			case POSTGRE_SQL:
				stmt.executeUpdate("analyze history");
				stmt.executeUpdate("analyze contracts");
				break;
			case OTHER:
				// なにもしない
				break;
			default:
				throw new AssertionError();
			}
			conn.commit();
		}
		long elapsedTime = System.currentTimeMillis() - startTime;
		String format = "Update statistic in %,.3f sec ";
		LOG.info(String.format(format, elapsedTime / 1000d));
	}
}
