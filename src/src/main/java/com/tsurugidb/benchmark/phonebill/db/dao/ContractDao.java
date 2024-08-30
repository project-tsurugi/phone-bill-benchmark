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
import java.util.Collection;
import java.util.List;

import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract.Key;

public interface ContractDao {
    static final String TABLE_NAME = "contracts";

    int[] batchInsert(Collection<Contract> contracts);
    int insert(Contract contract);
    int update(Contract contract);
    int delete(Key key);
    long count();
    List<Contract> getContracts(String phoneNumber);
    List<Contract> getContracts(Date start, Date end);
    List<Contract> getContracts();
    List<String> getAllPhoneNumbers();
    List<Key> getAllPrimaryKeys();
    Contract getContract(Key key);
}
