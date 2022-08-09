package com.tsurugidb.benchmark.phonebill.db.doma2.dao;

import java.sql.Date;

import com.tsurugidb.benchmark.phonebill.db.doma2.entity.Billing;

public interface BillingDao {
	public void insert(Billing billing);
	public void delete(Date targetMonth);

}
