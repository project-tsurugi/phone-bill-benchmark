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

/**
 * デバッグ用のContractDao
 *
 */
public class ContractDaoDebug implements ContractDao {
    private ContractDao dao;

    public ContractDaoDebug(ContractDao dao) {
        this.dao = dao;
    }

    @Override
    public int[] batchInsert(Collection<Contract> contracts) {
        return dao.batchInsert(contracts);
    }

    @Override
    public int insert(Contract contract) {
        return dao.insert(contract);
    }

    @Override
    public int update(Contract contract) {
        return dao.update(contract);
    }

    @Override
    public List<Contract> getContracts(String phoneNumber) {
        return dao.getContracts(phoneNumber);
    }

    @Override
    public List<Contract> getContracts(Date start, Date end) {
        return dao.getContracts(start, end);
    }

    @Override
    public List<Contract> getContracts() {
        return dao.getContracts();
    }

    @Override
    public List<String> getAllPhoneNumbers() {
        return dao.getAllPhoneNumbers();
    }

    @Override
    public long count() {
        return dao.count();
    }

    @Override
    public int delete(Key key) {
        return dao.delete(key);
    }

    @Override
    public List<Key> getAllPrimaryKeys() {
        return dao.getAllPrimaryKeys();
    }

    @Override
    public Contract getContract(Key key) {
        return dao.getContract(key);
    }
}
