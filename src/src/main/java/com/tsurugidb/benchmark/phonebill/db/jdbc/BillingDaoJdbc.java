package com.tsurugidb.benchmark.phonebill.db.jdbc;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.tsurugidb.benchmark.phonebill.db.doma2.dao.BillingDao;
import com.tsurugidb.benchmark.phonebill.db.doma2.entity.Billing;

public class BillingDaoJdbc implements BillingDao {
	private final PhoneBillDbManagerJdbc manager;

	public BillingDaoJdbc(PhoneBillDbManagerJdbc manager) {
		this.manager = manager;
	}

	@Override
	public void insert(Billing billing) {
		Connection conn = manager.getConnection();
		String sql = "insert into billing("
				+ "phone_number, target_month, basic_charge, metered_charge, billing_amount, batch_exec_id)"
				+ " values(?, ?, ?, ?, ?, ?)";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setString(1, billing.phoneNumber);
			ps.setDate(2, billing.targetMonth);
			ps.setInt(3, billing.basicCharge);
			ps.setInt(4, billing.meteredCharge);
			ps.setInt(5, billing.billingAmount);
			ps.setString(6, billing.batchExecId);
			ps.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void delete(Date targetMonth) {
		Connection conn = manager.getConnection();
		String sql = "delete from billing where target_month = ?";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setDate(1, targetMonth);
			ps.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

}
