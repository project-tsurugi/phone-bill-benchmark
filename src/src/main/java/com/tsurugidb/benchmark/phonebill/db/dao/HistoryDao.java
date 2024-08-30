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
package com.tsurugidb.benchmark.phonebill.db.dao;

import java.util.Collection;
import java.util.List;

import com.tsurugidb.benchmark.phonebill.app.billing.CalculationTarget;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract.Key;
import com.tsurugidb.benchmark.phonebill.db.entity.History;

public interface HistoryDao {
	static final String TABLE_NAME = "history";

	int[] batchInsert(Collection<History> histories);

	int insert(History history);

	long getMaxStartTime();

	int update(History history);

	int batchUpdate(List<History> histories);

    int updateNonKeyFields(History history);

    int batchUpdateNonKeyFields(List<History> histories);

	List<History> getHistories(Key key);

	List<History> getHistories(CalculationTarget target);

	List<History> getHistories();

	int updateChargeNull();

	int delete(String phoneNumber);

	int delete();

	List<String> getAllPhoneNumbers();

	long count();

	// RuntimeExceptionを発生させる => UT専用
	default void throwRuntimeException() {
		throw new RuntimeException();
	}

}
