package com.tsurugidb.benchmark.phonebill.db.doma2.dao;

import java.util.List;

import com.tsurugidb.benchmark.phonebill.db.doma2.entity.Contract;

public interface ContractDao {
	int[] batchInsert(List<Contract> contracts);
	int batchInsert(Contract contract);
	int update(Contract contract);
	List<Contract> getContracts(String phoneNumber);
	List<String> getAllPhoneNumbers();
}
