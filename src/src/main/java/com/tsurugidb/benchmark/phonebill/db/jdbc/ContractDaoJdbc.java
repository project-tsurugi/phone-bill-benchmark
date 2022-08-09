package com.tsurugidb.benchmark.phonebill.db.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.tsurugidb.benchmark.phonebill.db.doma2.dao.ContractDao;
import com.tsurugidb.benchmark.phonebill.db.doma2.entity.Contract;

public class ContractDaoJdbc implements ContractDao {
	private final PhoneBillDbManagerJdbc manager;

	private static final String SQL_INSERT = "insert into contracts("
			+ "phone_number,"
			+ "start_date,"
			+ "end_date,"
			+ "charge_rule"
			+ ") values(?, ?, ?, ?)";

	private static final String SQL_UPDATE =
			"update contracts set end_date = ?, charge_rule = ? where phone_number = ? and start_date = ?";

	private static final String SQL_SELECT_BY_PHONE_NUMBER =
			"select start_date, end_date, charge_rule from contracts where phone_number = ? order by start_date";

	private static final String SQL_SELECT_PHONE_NUMBER =
			"select phone_number from contracts order by phone_number";

	public ContractDaoJdbc(PhoneBillDbManagerJdbc manager) {
		this.manager = manager;
	}

	@Override
	public int[] batchInsert(List<Contract> contracts) {
		Connection conn = manager.getConnection();
		try (PreparedStatement ps = conn.prepareStatement(SQL_INSERT);) {
			for (Contract c : contracts) {
				setPsToContract(c, ps);
				ps.addBatch();
			}
			return ps.executeBatch();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public int batchInsert(Contract c) {
		Connection conn = manager.getConnection();
		try (PreparedStatement ps = conn.prepareStatement(SQL_INSERT);) {
				setPsToContract(c, ps);
			return ps.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * @param c
	 * @param ps
	 * @throws SQLException
	 */
	private void setPsToContract(Contract c, PreparedStatement ps) throws SQLException {
		ps.setString(1, c.phoneNumber);
		ps.setDate(2, c.startDate);
		ps.setDate(3, c.endDate);
		ps.setString(4, c.rule);
	}

	@Override
	public int update(Contract contract) {
		Connection conn = manager.getConnection();
		try (PreparedStatement ps = conn.prepareStatement(SQL_UPDATE)) {
			ps.setDate(1, contract.endDate);
			ps.setString(2, contract.rule);
			ps.setString(3, contract.phoneNumber);
			ps.setDate(4, contract.startDate);
			return ps.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<Contract> getContracts(String phoneNumber) {
		Connection conn = manager.getConnection();
		List<Contract> list = new ArrayList<>();
		try (PreparedStatement ps = conn.prepareStatement(SQL_SELECT_BY_PHONE_NUMBER)) {
			ps.setString(1, phoneNumber);
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					Contract c = new Contract();
					c.phoneNumber = phoneNumber;
					c.startDate = rs.getDate(1);
					c.endDate = rs.getDate(2);
					c.rule = rs.getString(3);
					list.add(c);
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return list;

	}

	@Override
	public List<String> getAllPhoneNumbers() {
		Connection conn = manager.getConnection();
		List<String> list = new ArrayList<>();
		try (PreparedStatement ps = conn.prepareStatement(SQL_SELECT_PHONE_NUMBER)) {
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					String phoneNumber = rs.getString(1);
					list.add(phoneNumber);
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return list;
	}
}
