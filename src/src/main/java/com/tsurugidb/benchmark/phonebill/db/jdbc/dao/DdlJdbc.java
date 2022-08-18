package com.tsurugidb.benchmark.phonebill.db.jdbc.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.db.dao.Ddl;
import com.tsurugidb.benchmark.phonebill.db.jdbc.PhoneBillDbManagerJdbc;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public abstract class DdlJdbc implements Ddl {
    protected static final Logger LOG = LoggerFactory.getLogger(DdlJdbc.class);
    private PhoneBillDbManagerJdbc managerJdbc;

	public DdlJdbc(PhoneBillDbManagerJdbc managerJdbc) {
		this.managerJdbc = managerJdbc;
	}

	@Override
	public void createContractsTable() {
		String create_table = "create table contracts ("
				+ "phone_number varchar(15) not null," 		// 電話番号
				+ "start_date date not null," 				// 契約開始日
				+ "end_date date,"							// 契約終了日
				+ "charge_rule varchar(255) not null"		// 料金計算ルール
				+ ")";
		execute(create_table);
	}

	@Override
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
		execute(create_table);
	}

	@Override
	public void afterLoadData() {
		createIndexes();
		updateStatistics();
	}

	@Override
	@SuppressFBWarnings(value = "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
	public void truncateTable(String tableName) {
		execute("truncate table " + tableName);
	}

	@Override
	@SuppressFBWarnings(value = "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
	public void createBackTable(String tableName) {
		execute("create table " + tableName + "_back as select * from " + tableName);
	}

	@Override
	@SuppressFBWarnings(value = "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
	public int count(String tableName) {
		try (Statement stmt = managerJdbc.getConnection().createStatement();
				ResultSet rs = stmt.executeQuery("select count(*) from " + tableName)) {
			if (rs.next()) {
				return rs.getInt(1);
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		throw new RuntimeException("No recoreds selected.");
	}

	/**
	 * 指定の文字列のSQLを実行する.
	 * <br>
	 * ignoreErrorCodeで指定したエラーコードのSQLExceptionが発生した場合、SQLExceptionを無視する。
	 *
	 * @param sql
	 * @param ignoreErrorCode
	 */
	protected void execute(String sql, int... errorCode) {
		try (Statement stmt = managerJdbc.getConnection().createStatement()){
			stmt.execute(sql);
			managerJdbc.commit();
		} catch (SQLException e) {
			managerJdbc.rollback();
			boolean ignore = false;
			for (int ec : errorCode) {
				if (e.getErrorCode() == ec) {
					ignore = true;
					break;
				}
			}
			if (!ignore) {
				throw new RuntimeException(e);
			}
		}
	}

	/**
	 * 実行前後にログを出力して指定の文字列のSQLを実行する.
	 * <br>
	 * ignoreErrorCodeで指定したエラーコードのSQLExceptionが発生した場合、SQLExceptionを無視する。
	 *
	 * @param sql
	 * @param erroceCode
	 */
	protected void executeWithLogging(String sql, int... erroceCode) {
		long startTime = System.currentTimeMillis();
		LOG.info("start exec sql:" + sql);
		execute(sql, erroceCode);
		long elapsedTime = System.currentTimeMillis() - startTime;
		String format = "end exec sql: " + sql + " in %,.3f sec ";
		LOG.info(String.format(format, elapsedTime / 1000d));
	}

	protected void commit() {
		try {
			managerJdbc.getConnection().commit();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public int countHistoryUpdated() {
		// ここで知りたいのはオンラインアプリで更新されたレコード数のため、バッチで更新されるカラムchargeは比較対象外
		return countDiff(
				"select caller_phone_number, recipient_phone_number, payment_categorty, start_time, time_secs, df from history_back",
				"select caller_phone_number, recipient_phone_number, payment_categorty, start_time, time_secs, df from history");
	}

	@Override
	public int countContractsUpdated() {
		return countDiff("select * from contracts_back", "select * from contracts");
	}


	/**
	 * 二つのqueryの差分をカウントする
	 *
	 * @param sql1
	 * @param sql2
	 * @return
	 */
	abstract protected int countDiff(String sql1, String sql2);

}
