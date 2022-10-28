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
import java.util.Collection;
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
	public int[] batchInsert(Collection<History> histories) {
		try (PreparedStatement ps = createInsertPs()) {
			for (History h : histories) {
				setHistoryToInsertPs(ps, h);
				ps.addBatch();
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
		ps.setString(1, h.getCallerPhoneNumber());
		ps.setString(2, h.getRecipientPhoneNumber());
		ps.setString(3, h.getPaymentCategorty());
		ps.setTimestamp(4, h.getStartTime());
		ps.setInt(5, h.getTimeSecs());
		if (h.getCharge() == null) {
			ps.setNull(6, Types.INTEGER);
		} else {
			ps.setInt(6, h.getCharge());
		}
		ps.setInt(7, h.getDf());
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
	public int updateChargeNull() {
		try (Statement stmt = manager.getConnection().createStatement()) {
			return stmt.executeUpdate("update history set charge = null");
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}


	@Override
	public int batchUpdate(List<History> list) {
		try (PreparedStatement ps = createUpdatePs()) {
			for(History h: list) {
				setHistroryToUpdatePs(h, ps);
				ps.addBatch();
			}
			int[] rets = ps.executeBatch();
			for (int ret : rets) {
				if (ret < 0 && ret != PreparedStatement.SUCCESS_NO_INFO) {
					throw new RuntimeException("Fail to update history.");
				}
			}
			return rets.length;
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
		ps.setString(1, history.getRecipientPhoneNumber());
		ps.setString(2, history.getPaymentCategorty());
		ps.setInt(3, history.getTimeSecs());
		if (history.getCharge() == null) {
			ps.setNull(4, Types.INTEGER);
		} else {
			ps.setInt(4, history.getCharge());
		}
		ps.setInt(5, history.getDf());
		ps.setString(6, history.getCallerPhoneNumber());
		ps.setTimestamp(7, history.getStartTime());
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
		String sql = "select" + " h.caller_phone_number, h.recipient_phone_number, h.payment_categorty, h.start_time, h.time_secs,"
				+ " h.charge, h.df" + " from history h"
				+ " inner join contracts c on c.phone_number = h.caller_phone_number"
				+ " where c.start_date <= h.start_time and" + " (h.start_time < c.end_date + 1"
				+ " or c.end_date is null)" + " and c.phone_number = ? and c.start_date = ?" + " order by h.start_time";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setString(1, key.getPhoneNumber());
			ps.setDate(2, key.getStartDate());
			return createHistoriesLlist(ps);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public List<History> getHistories(CalculationTarget target) {
		Connection conn = manager.getConnection();
		Contract contract = target.getContract();
		Date start = target.getStart();
		Date end = target.getEnd();
		String sql = "select caller_phone_number, recipient_phone_number, payment_categorty, start_time, time_secs,"
				+ " charge, df" + " from history "
				+ "where start_time >= ? and start_time < ?"
				+ " and ((caller_phone_number = ? and payment_categorty = 'C') "
				+ "  or (recipient_phone_number = ? and payment_categorty = 'R'))"
				+ " and df = 0";

		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setDate(1, start);
			ps.setDate(2, DateUtils.nextDate(end));
			ps.setString(3, contract.getPhoneNumber());
			ps.setString(4, contract.getPhoneNumber());
			return createHistoriesLlist(ps);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public List<History> getHistories() {
		Connection conn = manager.getConnection();
		String sql = "select caller_phone_number, recipient_phone_number, payment_categorty, start_time, time_secs,"
				+ " charge, df" + " from history ";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			return createHistoriesLlist(ps);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * @param list
	 * @param psSelect
	 * @throws SQLException
	 */
	private List<History> createHistoriesLlist(PreparedStatement psSelect) throws SQLException {
		List<History> list = new ArrayList<History>();
		try (ResultSet rs = psSelect.executeQuery()) {
			while (rs.next()) {
				History h = new History();
				h.setCallerPhoneNumber(rs.getString(1));
				h.setRecipientPhoneNumber(rs.getString(2));
				h.setPaymentCategorty(rs.getString(3));
				h.setStartTime(rs.getTimestamp(4));
				h.setTimeSecs(rs.getInt(5));
				h.setCharge(rs.getInt(6));
				if (rs.wasNull()) {
					h.setCharge(null);
				}
				h.setDf(rs.getInt(7));
				list.add(h);
			}
		}
		return list;
	}
}