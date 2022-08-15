package com.tsurugidb.benchmark.phonebill.db.dao;

import java.sql.Date;

import com.tsurugidb.benchmark.phonebill.db.entity.Billing;

public interface BillingDao {
	public void insert(Billing billing);
	public void delete(Date targetMonth);

}
