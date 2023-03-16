package com.tsurugidb.benchmark.phonebill.db.dao;

import java.sql.Date;
import java.util.Collection;
import java.util.List;

import com.tsurugidb.benchmark.phonebill.db.entity.Contract;

public interface ContractDao {
	static final String TABLE_NAME = "contracts";

	int[] batchInsert(Collection<Contract> contracts);
	int insert(Contract contract);
	int update(Contract contract);
	long count();
	List<Contract> getContracts(String phoneNumber);
	List<Contract> getContracts(Date start, Date end);
	List<Contract> getContracts();
	List<String> getAllPhoneNumbers();
}
