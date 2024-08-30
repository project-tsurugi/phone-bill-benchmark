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

import java.sql.Date;
import java.util.List;

import com.tsurugidb.benchmark.phonebill.db.entity.Billing;

public interface BillingDao {
	static final String TABLE_NAME = "billing";

	public int insert(Billing billing);
	public int delete(Date targetMonth);
	public int delete();
	public List<Billing> getBillings();


}
