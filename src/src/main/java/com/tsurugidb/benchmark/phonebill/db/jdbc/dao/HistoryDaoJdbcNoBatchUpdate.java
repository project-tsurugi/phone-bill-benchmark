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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.benchmark.phonebill.db.jdbc.PhoneBillDbManagerJdbc;

public class HistoryDaoJdbcNoBatchUpdate extends HistoryDaoJdbc {

	public HistoryDaoJdbcNoBatchUpdate(PhoneBillDbManagerJdbc manager) {
		super(manager);
	}

	@Override
	public int[] batchInsert(Collection<History> histories) {
		try (PreparedStatement ps = createInsertPs()) {
			int[] rets = new int[histories.size()];
			int idx = 0;
			for (History h : histories) {
				setHistoryToInsertPs(ps, h);
				rets[idx++] = ps.executeUpdate();
			}
			return rets;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public int batchUpdate(List<History> list) {
		try (PreparedStatement ps = createUpdatePs()) {
			for(History h: list) {
				setHistroryToUpdatePs(h, ps);
				int ret = ps.executeUpdate();
				if (ret < 0) {
					throw new RuntimeException("Fail to update history.");
				}
			}
			return list.size();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}




}
