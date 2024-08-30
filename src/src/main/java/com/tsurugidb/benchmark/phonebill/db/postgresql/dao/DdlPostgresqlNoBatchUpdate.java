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
package com.tsurugidb.benchmark.phonebill.db.postgresql.dao;

import com.tsurugidb.benchmark.phonebill.db.jdbc.PhoneBillDbManagerJdbc;

/**
 * セカンダリインデックスを指定できないTsurugiと条件を揃えるために
 * セカンダリインデックスを使用しない。
 */
public class DdlPostgresqlNoBatchUpdate extends DdlPostgresql {

	public DdlPostgresqlNoBatchUpdate(PhoneBillDbManagerJdbc manager) {
		super(manager);
	}

	@Override
	public void createIndexes() {
		createIndexes(false);
	}
}
