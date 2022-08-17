package com.tsurugidb.benchmark.phonebill.db.dao;

import java.sql.Date;
import java.util.List;

import com.tsurugidb.benchmark.phonebill.db.entity.Contract;

public interface ContractDao {
	int batchInsert(List<Contract> contracts);
	int inserf(Contract contract);
	int update(Contract contract);
	List<Contract> getContracts(String phoneNumber);
	List<Contract> getContracts(Date start, Date end);
	List<String> getAllPhoneNumbers();
}
