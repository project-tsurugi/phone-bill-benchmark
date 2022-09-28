package com.tsurugidb.benchmark.phonebill.db.jdbc.dao;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.tsurugidb.benchmark.phonebill.db.dao.ContractDao;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.jdbc.PhoneBillDbManagerJdbc;

public class ContractDaoJdbc implements ContractDao {
	private final PhoneBillDbManagerJdbc manager;

	private static final String SQL_INSERT = "insert into contracts("
			+ "phone_number,"
			+ "start_date,"
			+ "end_date,"
			+ "charge_rule"
			+ ") values(?, ?, ?, ?)";

	public ContractDaoJdbc(PhoneBillDbManagerJdbc manager) {
		this.manager = manager;
	}

	@Override
	public int[] batchInsert(Collection<Contract> contracts) {
		Connection conn = manager.getConnection();
		try (PreparedStatement ps = conn.prepareStatement(SQL_INSERT);) {
			for (Contract c : contracts) {
				setPsToContract(c, ps);
				ps.addBatch();
			}
			int[] rets = ps.executeBatch();
			for (int ret : rets) {
				if (ret < 0 && ret != PreparedStatement.SUCCESS_NO_INFO) {
					throw new RuntimeException("Fail to batch insert to contracts.");
				}
			}
			return rets;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public int insert(Contract c) {
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
		ps.setString(1, c.getPhoneNumber());
		ps.setDate(2, c.getStartDate());
		ps.setDate(3, c.getEndDate());
		ps.setString(4, c.getRule());
	}

	@Override
	public int update(Contract contract) {
		Connection conn = manager.getConnection();
		String sql = "update contracts set end_date = ?, charge_rule = ? where phone_number = ? and start_date = ?";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setDate(1, contract.getEndDate());
			ps.setString(2, contract.getRule());
			ps.setString(3, contract.getPhoneNumber());
			ps.setDate(4, contract.getStartDate());
			return ps.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<Contract> getContracts(String phoneNumber) {
		Connection conn = manager.getConnection();
		List<Contract> list = new ArrayList<>();
		String sql = "select start_date, end_date, charge_rule from contracts where phone_number = ? order by start_date";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setString(1, phoneNumber);
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					Contract c = new Contract();
					c.setPhoneNumber(phoneNumber);
					c.setStartDate(rs.getDate(1));
					c.setEndDate(rs.getDate(2));
					c.setRule(rs.getString(3));
					list.add(c);
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return list;

	}

	@Override
	public List<Contract> getContracts(Date start, Date end) {
		Connection conn = manager.getConnection();
		String sql = "select phone_number, start_date, end_date, charge_rule"
				+ " from contracts where start_date <= ? and ( end_date is null or end_date >= ?)"
				+ " order by phone_number";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setDate(1, end);
			ps.setDate(2, start);
			try (ResultSet rs = ps.executeQuery()) {
				return createContractList(rs);
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<Contract> getContracts() {
		Connection conn = manager.getConnection();
		String sql = "select phone_number, start_date, end_date, charge_rule from contracts";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			try (ResultSet rs = ps.executeQuery()) {
				return createContractList(rs);
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * @param rs
	 * @return
	 * @throws SQLException
	 */
	private List<Contract> createContractList(ResultSet rs) throws SQLException {
		List<Contract> list = new ArrayList<>();
		while (rs.next()) {
			Contract contract = new Contract();
			contract.setPhoneNumber(rs.getString(1));
			contract.setStartDate(rs.getDate(2));
			contract.setEndDate(rs.getDate(3));
			contract.setRule(rs.getString(4));
			list.add(contract);
		}
		return list;
	}


	@Override
	public List<String> getAllPhoneNumbers() {
		Connection conn = manager.getConnection();
		List<String> list = new ArrayList<>();
		String sql = "select phone_number from contracts order by phone_number";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
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
