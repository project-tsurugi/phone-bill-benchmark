package com.tsurugidb.benchmark.phonebill.db.postgresql.dao;

import com.tsurugidb.benchmark.phonebill.db.jdbc.PhoneBillDbManagerJdbc;

/**
 * セカンダリインデックスを指定できないTsurugiと条件を揃えるために
 * セカンダリインデックスを使用しない。
 */
public class DdlPostgresqlNoBatchUpdate extends DdlPostgresql {

	public DdlPostgresqlNoBatchUpdate(PhoneBillDbManagerJdbc manager) {
		super(manager);
	}

	@Override
	public void createIndexes() {
		createIndexes(false);
	}
}
