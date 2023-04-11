package com.tsurugidb.benchmark.phonebill;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager.SessionHoldingType;
import com.tsurugidb.benchmark.phonebill.db.entity.Billing;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.benchmark.phonebill.db.jdbc.PhoneBillDbManagerJdbc;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * JDBCでDBにアクセスするテストケース共通用のクラス。
 *
 */
public abstract class AbstractJdbcTestCase {
	private static PhoneBillDbManager manager;
	private static Connection conn;
	private static Statement stmt;

	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		manager = PhoneBillDbManager.createPhoneBillDbManager(Config.getConfig(), SessionHoldingType.INSTANCE_FIELD);
		conn = ((PhoneBillDbManagerJdbc)manager).getConnection();
		conn.setAutoCommit(true);
		stmt = conn.createStatement();
	}

	@AfterAll
	static void tearDownAfterClass() throws Exception {
		conn.close();
		manager.close();
	}

	@BeforeEach
	void beforeTest() throws SQLException {
		conn.setAutoCommit(false);
		manager.getDdl().dropTables();
		manager.getDdl().createBillingTable();
		manager.getDdl().createContractsTable();
		manager.getDdl().createHistoryTable();
		conn.setAutoCommit(true);
	}

	@AfterEach
	void afterTest() throws SQLException {
		if (!conn.getAutoCommit()) {
			conn.rollback();
		}
	}


	@SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
	protected void truncateTable(String tableName) throws SQLException {
		String sql = "truncate table " + tableName;
		stmt.executeUpdate(sql);
	}

	@SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
	protected int countRecords(String tableName) throws SQLException {
		String sql = "select count(*) from " + tableName;
		try (ResultSet rs = stmt.executeQuery(sql)) {
			if (rs.next()) {
				return rs.getInt(1);
			} else {
				throw new RuntimeException("No records selected.");
			}
		}
	}


	protected List<History> getHistories(PhoneBillDbManager manager) throws SQLException {
		try  (Statement  stmt = ((PhoneBillDbManagerJdbc) manager).getConnection().createStatement()) {
			return getHistories(stmt);
		}
	}

	protected Set<History> getHistorySet() throws SQLException {
		return new HashSet<History>(getHistories());
	}


	protected List<History> getHistories() throws SQLException {
		return getHistories(AbstractJdbcTestCase.stmt);
	}

	/**
	 * @param stmt
	 * @return
	 * @throws SQLException
	 */
	private List<History> getHistories(Statement stmt) throws SQLException {
		List<History> list = new ArrayList<History>();
		String sql = "select caller_phone_number, recipient_phone_number,"
				+ " payment_category, start_time,time_secs,charge, df"
				+ " from history order by caller_phone_number, start_time";
		try (ResultSet rs = stmt.executeQuery(sql)) {
			while (rs.next()) {
				History history = new History();
				history.setCallerPhoneNumber(rs.getString(1));
				history.setRecipientPhoneNumber(rs.getString(2));
				history.setPaymentCategorty(rs.getString(3));
				history.setStartTime(rs.getTimestamp(4));
				history.setTimeSecs(rs.getInt(5));
				history.setCharge(rs.getInt(6));
				if (rs.wasNull()) {
					history.setCharge(null);
				}
				history.setDf(rs.getInt(7));
				list.add(history);
			}
		}
		return list;
	}



	protected History toHistory(String caller_phone_number, String recipient_phone_number, String payment_category,
			String start_time, int time_secs, Integer charge,
			int df) {
		History history = new History();
		history.setCallerPhoneNumber(caller_phone_number);
		history.setRecipientPhoneNumber(recipient_phone_number);
		history.setPaymentCategorty(payment_category);
		history.setStartTime(DateUtils.toTimestamp(start_time));
		history.setTimeSecs(time_secs);
		history.setCharge(charge);
		history.setDf(df);
		return history;
	}

	protected List<Contract> getContracts() throws SQLException {
		List<Contract> contracts = new ArrayList<Contract>();
		String sql = "select phone_number, start_date, end_date, charge_rule"
				+ " from contracts order by phone_number, start_date";
		try (ResultSet rs = getStmt().executeQuery(sql)) {
			while (rs.next()) {
				Contract c = new Contract();
				c.setPhoneNumber(rs.getString(1));
				c.setStartDate(rs.getDate(2));
				c.setEndDate(rs.getDate(3));
				c.setRule(rs.getString(4));
				contracts.add(c);
			}
		}
		return contracts;
	}

	protected Set<Contract> getContractSet() throws SQLException {
		return new HashSet<>(getContracts());
	}


	protected void executeSql(String sql) throws SQLException {
		stmt.execute(sql);
	}

	/**
	 * @return conn
	 */
	protected static Connection getConn() {
		return conn;
	}

	/**
	 * @return stmt
	 */
	protected static Statement getStmt() {
		return stmt;
	}

	/**
	 * @return manager
	 */
	protected static PhoneBillDbManager getManager() {
		return manager;
	}

	/**
	 * 契約マスタにレコードを追加する
	 *
	 * @param phoneNumber
	 * @param startDate
	 * @param endDate
	 * @param chargeRule
	 * @throws SQLException
	 */
	protected void insertToContracts(String phoneNumber, String startDate, String endDate, String chargeRule)
			throws SQLException {
		String sql = "insert into contracts(phone_number, start_date, end_date, charge_rule) values(?, ?, ?, ?)";
		try (PreparedStatement ps = getConn().prepareStatement(sql)) {
			ps.setString(1, phoneNumber);
			ps.setDate(2, DateUtils.toDate(startDate));
			if (endDate == null) {
				ps.setNull(3, Types.DATE);
			} else {
				ps.setDate(3, DateUtils.toDate(endDate));
			}
			ps.setString(4, chargeRule);
			int c = ps.executeUpdate();
			assertEquals(1, c);
		}
	}

	protected List<Billing> getBillings() throws SQLException {
		List<Billing> list = new ArrayList<Billing>();
		String sql = "select phone_number, target_month, basic_charge, metered_charge, billing_amount"
				+ " from billing order by phone_number, target_month";
		try (ResultSet rs = getStmt().executeQuery(sql)) {
			while (rs.next()) {
				Billing billing = new Billing();
				billing.setPhoneNumber(rs.getString(1));
				billing.setTargetMonth(rs.getDate(2));
				billing.setBasicCharge(rs.getInt(3));
				billing.setMeteredCharge(rs.getInt(4));
				billing.setBillingAmount(rs.getInt(5));
				list.add(billing);
			}
		}
		return list;
	}


	/**
	 * 履歴テーブルにレコードを追加する
	 *
	 * @param caller_phone_number 発信者電話番号
	 * @param recipient_phone_number 受信者電話番号
	 * @param payment_category	料金区分
	 * @param start_time 通話開始時刻
	 * @param time_secs 通話時間
	 * @param df 論理削除フラグ
	 * @throws SQLException
	 */
	protected void insertToHistory(String caller_phone_number, String recipient_phone_number, String payment_category, String start_time, Integer time_secs, int df)
			throws SQLException {
		String sql = "insert into history(caller_phone_number, recipient_phone_number, payment_category, start_time, time_secs, charge, df) values(?, ?, ?, ?, ?, ?, ?)";
		try (PreparedStatement ps = getConn().prepareStatement(sql)) {
			ps.setString(1, caller_phone_number);
			ps.setString(2, recipient_phone_number);
			ps.setString(3, payment_category);
			ps.setTimestamp(4, DateUtils.toTimestamp(start_time));
			ps.setInt(5, time_secs);
			ps.setNull(6, Types.INTEGER);
			ps.setInt(7, df);
			int c = ps.executeUpdate();
			assertEquals(1, c);
		}
	}

}
