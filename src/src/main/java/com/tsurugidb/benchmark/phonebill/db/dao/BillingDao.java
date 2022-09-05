package com.tsurugidb.benchmark.phonebill.db.dao;

import java.sql.Date;

import com.tsurugidb.benchmark.phonebill.db.entity.Billing;

public interface BillingDao {
	public int insert(Billing billing);
	public int delete(Date targetMonth);

}
