package com.tsurugidb.benchmark.phonebill.db.jdbc.dao;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import com.tsurugidb.benchmark.phonebill.app.billing.CalculationTarget;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract.Key;
import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.benchmark.phonebill.db.jdbc.PhoneBillDbManagerJdbc;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;

public class HistoryDaoJdbc implements HistoryDao {
	private final PhoneBillDbManagerJdbc manager;


	public HistoryDaoJdbc(PhoneBillDbManagerJdbc manager) {
		this.manager = manager;
	}

	@Override
	public int[] batchInsert(List<History> histories) {
		try (PreparedStatement ps = createInsertPs()) {
			for (History h : histories) {
				setHistoryToInsertPs(ps, h);
			}
			return ps.executeBatch();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public int insert(History history) {
		try (PreparedStatement ps = createInsertPs()) {
			setHistoryToInsertPs(ps, history);
			return ps.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public long getMaxStartTime() {
		Connection conn = manager.getConnection();
		try (Statement stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery("select max(start_time) from history");) {
			if (rs.next()) {
				Timestamp ts = rs.getTimestamp(1);
				return ts == null ? 0 : ts.getTime();
			} else {
				return 0;
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * @param ps
	 * @param h
	 * @throws SQLException
	 */
	private void setHistoryToInsertPs(PreparedStatement ps, History h) throws SQLException {
		ps.setString(1, h.callerPhoneNumber);
		ps.setString(2, h.recipientPhoneNumber);
		ps.setString(3, h.paymentCategorty);
		ps.setTimestamp(4, h.startTime);
		ps.setInt(5, h.timeSecs);
		if (h.charge == null) {
			ps.setNull(6, Types.INTEGER);
		} else {
			ps.setInt(6, h.charge);
		}
		ps.setInt(7, h.df);
		ps.addBatch();
	}


	private PreparedStatement createInsertPs() throws SQLException {
		PreparedStatement ps = manager.getConnection().prepareStatement("insert into history("
		+ "caller_phone_number,"
		+ "recipient_phone_number,"
		+ "payment_categorty,"
		+ "start_time,"
		+ "time_secs,"
		+ "charge,"
		+ "df"
		+ ") values(?, ?, ?, ?, ?, ?, ? )");
		return ps;
	}

	@Override
	public int update(History history) {
		try (PreparedStatement ps = createUpdatePs()) {
			setHistroryToUpdatePs(history, ps);
			return ps.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public int[] batchUpdate(List<History> list) {
		try (PreparedStatement ps = createUpdatePs()) {
			for(History h: list) {
				setHistroryToUpdatePs(h, ps);
				ps.addBatch();
			}
			return ps.executeBatch();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}


	/**
	 * @param history
	 * @param ps
	 * @throws SQLException
	 */
	private void setHistroryToUpdatePs(History history, PreparedStatement ps) throws SQLException {
		ps.setString(1, history.recipientPhoneNumber);
		ps.setString(2, history.paymentCategorty);
		ps.setInt(3, history.timeSecs);
		if (history.charge == null) {
			ps.setNull(4, Types.INTEGER);
		} else {
			ps.setInt(4, history.charge);
		}
		ps.setInt(5, history.df);
		ps.setString(6, history.callerPhoneNumber);
		ps.setTimestamp(7, history.startTime);
	}

	private PreparedStatement createUpdatePs() throws SQLException {
		PreparedStatement ps = manager.getConnection().prepareStatement(
				"update history"
				+ " set recipient_phone_number = ?, payment_categorty = ?, time_secs = ?, charge = ?, df = ?"
				+ " where caller_phone_number = ? and start_time = ?");
		return ps;
	}



	@Override
	public List<History> getHistories(Key key) {
		Connection conn = manager.getConnection();
		List<History> list = new ArrayList<History>();
		String sql = "select" + " h.recipient_phone_number, h.payment_categorty, h.start_time, h.time_secs,"
				+ " h.charge, h.df" + " from history h"
				+ " inner join contracts c on c.phone_number = h.caller_phone_number"
				+ " where c.start_date < h.start_time and" + " (h.start_time < c.end_date + 1"
				+ " or c.end_date is null)" + " and c.phone_number = ? and c.start_date = ?" + " order by h.start_time";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setString(1, key.phoneNumber);
			ps.setDate(2, key.startDate);
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					History h = new History();
					h.callerPhoneNumber = key.phoneNumber;
					h.recipientPhoneNumber = rs.getString(1);
					h.paymentCategorty = rs.getString(2);
					h.startTime = rs.getTimestamp(3);
					h.timeSecs = rs.getInt(4);
					h.charge = rs.getInt(5);
					if (rs.wasNull()) {
						h.charge = null;
					}
					h.df = rs.getInt(6);
					list.add(h);
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return list;
	}

	public List<History> getHistories(CalculationTarget target) {
		Connection conn = manager.getConnection();
		Contract contract = target.getContract();
		Date start = target.getStart();
		Date end = target.getEnd();
		List<History> list = new ArrayList<History>();
		String sql = "select caller_phone_number, recipient_phone_number, payment_categorty, start_time, time_secs,"
				+ " charge, df" + " from history "
				+ "where start_time >= ? and start_time < ?"
				+ " and ((caller_phone_number = ? and payment_categorty = 'C') "
				+ "  or (recipient_phone_number = ? and payment_categorty = 'R'))"
				+ " and df = 0";

		try (PreparedStatement psSelect = conn.prepareStatement(sql)) {
			psSelect.setDate(1, start);
			psSelect.setDate(2, DateUtils.nextDate(end));
			psSelect.setString(3, contract.phoneNumber);
			psSelect.setString(4, contract.phoneNumber);
			try (ResultSet rs = psSelect.executeQuery()) {
				while (rs.next()) {
					History h = new History();
					h.callerPhoneNumber = rs.getString(1);
					h.recipientPhoneNumber = rs.getString(2);
					h.paymentCategorty = rs.getString(3);
					h.startTime = rs.getTimestamp(4);
					h.timeSecs = rs.getInt(5);
					h.charge = rs.getInt(6);
					if (rs.wasNull()) {
						h.charge = null;
					}
					h.df = rs.getInt(7);
					list.add(h);
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return list;
	}
}