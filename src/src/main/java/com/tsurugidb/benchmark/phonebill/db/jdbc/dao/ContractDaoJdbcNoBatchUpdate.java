/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
