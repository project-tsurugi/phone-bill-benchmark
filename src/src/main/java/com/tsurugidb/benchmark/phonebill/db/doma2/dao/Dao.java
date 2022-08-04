package com.tsurugidb.benchmark.phonebill.db.doma2.dao;

import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;

/**
 * すべてのDAOの基底クラス
 */
public abstract class Dao {
	private PhoneBillDbManager manager;

	public Dao(PhoneBillDbManager manager) {
		this.manager = manager;
	}

	/**
	 * @return manager
	 */
	protected PhoneBillDbManager getManager() {
		return manager;
	}
}
