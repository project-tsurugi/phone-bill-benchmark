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
