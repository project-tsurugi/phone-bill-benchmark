package com.tsurugidb.benchmark.phonebill.db.jdbc;

import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.db.interfaces.DdlLExecutor;

public abstract class DdlExectorJdbc implements DdlLExecutor {
    protected static final Logger LOG = LoggerFactory.getLogger(DdlExectorJdbc.class);
    private PhoneBillDbManagerJdbc manager;

	public DdlExectorJdbc(PhoneBillDbManagerJdbc manager) {
		this.manager = manager;
	}

	public void createContractsTable() {
		String create_table = "create table contracts ("
				+ "phone_number varchar(15) not null," 		// 電話番号
				+ "start_date date not null," 				// 契約開始日
				+ "end_date date,"							// 契約終了日
				+ "charge_rule varchar(255) not null"		// 料金計算ルール
				+ ")";
		try (Statement stmt = manager.getConnection().createStatement()){
			stmt.execute(create_table);
			manager.commit();
		} catch (SQLException e) {
			manager.rollback();
			throw new RuntimeException(e);
		}
	}

	public void createBillingTable() {
		String create_table = "create table billing ("
				+ "phone_number varchar(15) not null," 					// 電話番号
				+ "target_month date not null," 						// 対象年月
				+ "basic_charge integer not null," 						// 基本料金
				+ "metered_charge integer not null,"					// 従量料金
				+ "billing_amount integer not null,"					// 請求金額
				+ "batch_exec_id varchar(36) not null,"					// バッチ実行ID
				+ "constraint  billing_pkey primary key(target_month, phone_number, batch_exec_id)"
				+ ")";
		try (Statement stmt = manager.getConnection().createStatement()){
			stmt.execute(create_table);
			manager.commit();
		} catch (SQLException e) {
			manager.rollback();
			throw new RuntimeException(e);
		}
	}

	public void afterLoadData() {
		createIndexes();
		updateStatistics();
	}


	/*
	 * 指定のSQLを実行前後にログを入れて実行する
	 */
	protected void execSql(Statement stmt, String sql) throws SQLException {
		long startTime = System.currentTimeMillis();
		LOG.info("start exec sql:" + sql);
		stmt.execute(sql);
		long elapsedTime = System.currentTimeMillis() - startTime;
		String format = "end exec sql: " + sql + " in %,.3f sec ";
		LOG.info(String.format(format, elapsedTime / 1000d));
	}
}
