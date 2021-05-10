package com.example.nedo;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import com.example.nedo.app.Config;
import com.example.nedo.db.DBUtils;
import com.example.nedo.db.History;

/**
 * DBにアクセスするテストケース共通用のクラス。
 *
 */
public abstract class AbstractDbTestCase {
	protected static Connection conn;
	protected static Statement stmt;

	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		conn = DBUtils.getConnection(Config.getConfig());
		conn.setAutoCommit(true);
		stmt = conn.createStatement();
	}

	@AfterAll
	static void tearDownAfterClass() throws Exception {
		conn.close();
	}

	protected void truncateTable(String tableName) throws SQLException {
		String sql = "truncate table " + tableName;
		stmt.executeUpdate(sql);
	}

	protected int countRecords(String tableName) throws SQLException {
		String sql = "select count(*) from " + tableName;
		ResultSet rs = stmt.executeQuery(sql);
		if (rs.next()) {
			return rs.getInt(1);
		} else {
			throw new RuntimeException("No records selected.");
		}
	}

	protected List<History> getHistories() throws SQLException {
		List<History> list = new ArrayList<History>();
		String sql = "select caller_phone_number, recipient_phone_number,"
				+ " payment_categorty, start_time,time_secs,charge, df"
				+ " from history order by caller_phone_number, start_time";
		ResultSet rs = stmt.executeQuery(sql);
		while (rs.next()) {
			History history = new History();
			history.callerPhoneNumber = rs.getString(1);
			history.recipientPhoneNumber = rs.getString(2);
			history.paymentCategorty = rs.getString(3);
			history.startTime = rs.getTimestamp(4);
			history.timeSecs = rs.getInt(5);
			history.charge = rs.getInt(6);
			if (rs.wasNull()) {
				history.charge = null;
			}
			history.df = rs.getBoolean(7);
			list.add(history);
		}
		return list;
	}

	protected History toHistory(String caller_phone_number, String recipient_phone_number, String payment_categorty,
			String start_time, int time_secs, Integer charge,
			boolean df) {
		History history = new History();
		history.callerPhoneNumber = caller_phone_number;
		history.recipientPhoneNumber = recipient_phone_number;
		history.paymentCategorty = payment_categorty;
		history.startTime = DBUtils.toTimestamp(start_time);
		history.timeSecs = time_secs;
		history.charge = charge;
		history.df = df;
		return history;
	}

	protected void executeSql(String sql) throws SQLException {
		stmt.execute(sql);
	}
}
