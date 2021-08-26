package com.example.nedo.app;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.example.nedo.app.Config.Dbms;
import com.example.nedo.db.DBUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class CreateTable implements ExecutableCommand{
	private boolean isOracle;
	private Config config;

	public static void main(String[] args) throws Exception {
		Config config = Config.getConfig(args);
		CreateTable createTable = new CreateTable();
		createTable.execute(config);
	}

	@Override
	public void execute(Config config) throws Exception {
		this.config = config;
		isOracle = config.dbms == Dbms.ORACLE;
		try (Connection conn = DBUtils.getConnection(config);
				Statement stmt = conn.createStatement()) {
			conn.setAutoCommit(true);
			dropTables(stmt);
			createHistoryTable(stmt);
			createContractsTable(stmt);
			createBillingTable(stmt);
		}
	}

	void createHistoryTable(Statement stmt) throws SQLException {
		String create_table = "create table history ("
				+ "caller_phone_number varchar(15) not null," 		// 発信者電話番号
				+ "recipient_phone_number varchar(15) not null," 	// 受信者電話番号
				+ "payment_categorty char(1) not null," 			// 料金区分
				+ "start_time timestamp not null,"			 		// 通話開始時刻
				+ "time_secs integer not null," 					// 通話時間(秒)
				+ "charge integer," 								// 料金
				+ "df integer not null," 							// 論理削除フラグ
				+ "constraint history_pkey primary key(caller_phone_number, start_time)"
				+ ")";
		if (isOracle && config.oracleInitran != 0) {
			create_table = create_table + "initrans " + config.oracleInitran;
		}
		stmt.execute(create_table);

		String create_index_df = "create index idx_df on history(df)";
		stmt.execute(create_index_df);
		String create_index_st = "create index idx_st on history(start_time)";
		stmt.execute(create_index_st);
		String create_index_rp = "create index rp on history(recipient_phone_number, start_time)";
		stmt.execute(create_index_rp);
	}

	void createContractsTable(Statement stmt) throws SQLException {
		String create_table = "create table contracts ("
				+ "phone_number varchar(15) not null," 		// 電話番号
				+ "start_date date not null," 				// 契約開始日
				+ "end_date date,"							// 契約終了日
				+ "charge_rule varchar(255) not null,"		// 料金計算ルール
				+ "primary key(phone_number, start_date)"
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


	void dropTables(Statement stmt) throws SQLException {
		// 通話履歴テーブル
		if (isOracle) {
			dropTableOracle(stmt, "history");
			dropTableOracle(stmt, "contracts");
			dropTableOracle(stmt, "billing");
		} else {
			dropTable(stmt, "history");
			dropTable(stmt, "contracts");
			dropTable(stmt, "billing");
		}
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

}
