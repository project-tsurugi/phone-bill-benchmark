package com.tsurugidb.benchmark.phonebill.db.jdbc.dao;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.tsurugidb.benchmark.phonebill.db.dao.BillingDao;
import com.tsurugidb.benchmark.phonebill.db.entity.Billing;
import com.tsurugidb.benchmark.phonebill.db.jdbc.PhoneBillDbManagerJdbc;

public class BillingDaoJdbc implements BillingDao {
	private final PhoneBillDbManagerJdbc manager;

	public BillingDaoJdbc(PhoneBillDbManagerJdbc manager) {
		this.manager = manager;
	}

	@Override
	public int insert(Billing billing) {
		Connection conn = manager.getConnection();
		String sql = "insert into billing("
				+ "phone_number, target_month, basic_charge, metered_charge, billing_amount, batch_exec_id)"
				+ " values(?, ?, ?, ?, ?, ?)";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setString(1, billing.getPhoneNumber());
			ps.setDate(2, billing.getTargetMonth());
			ps.setInt(3, billing.getBasicCharge());
			ps.setInt(4, billing.getMeteredCharge());
			ps.setInt(5, billing.getBillingAmount());
			ps.setString(6, billing.getBatchExecId());
			return ps.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public int delete(Date targetMonth) {
		Connection conn = manager.getConnection();
		String sql = "delete from billing where target_month = ?";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setDate(1, targetMonth);
			return ps.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<Billing> getBillings() {
		List<Billing> list = new ArrayList<>();
		Connection conn = manager.getConnection();
		String sql = "select phone_number, target_month, basic_charge, metered_charge, billing_amount, batch_exec_id from billing";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					Billing b  = new Billing();
					b.setPhoneNumber(rs.getString(1));
					b.setTargetMonth(rs.getDate(2));
					b.setBasicCharge(rs.getInt(3));
					b.setMeteredCharge(rs.getInt(4));
					b.setBillingAmount(rs.getInt(5));
					b.setBatchExecId(rs.getString(6));
					list.add(b);
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return list;
	}

	@Override
	public int delete() {
		Connection conn = manager.getConnection();
		String sql = "delete from billing";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			return ps.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

}
