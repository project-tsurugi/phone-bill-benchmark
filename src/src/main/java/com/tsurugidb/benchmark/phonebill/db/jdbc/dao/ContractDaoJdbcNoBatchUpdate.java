package com.tsurugidb.benchmark.phonebill.db.jdbc.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;

import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.jdbc.PhoneBillDbManagerJdbc;

public class ContractDaoJdbcNoBatchUpdate extends ContractDaoJdbc {

	public ContractDaoJdbcNoBatchUpdate(PhoneBillDbManagerJdbc manager) {
		super(manager);
	}

	@Override
	public int[] batchInsert(Collection<Contract> contracts) {
		Connection conn = manager.getConnection();
		try (PreparedStatement ps = conn.prepareStatement(SQL_INSERT);) {
			int[] rets = new int[contracts.size()];
			int idx = 0;
			for (Contract c : contracts) {
				setPsToContract(c, ps);
				rets[idx++] = ps.executeUpdate();
			}
			for (int ret : rets) {
				if (ret < 1) {
					throw new RuntimeException("Fail to batch insert to contracts.");
				}
			}
			return rets;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
