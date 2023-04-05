package com.tsurugidb.benchmark.phonebill.db.postgresql;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.dao.ContractDao;
import com.tsurugidb.benchmark.phonebill.db.dao.Ddl;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;
import com.tsurugidb.benchmark.phonebill.db.jdbc.dao.ContractDaoJdbcNoBatchUpdate;
import com.tsurugidb.benchmark.phonebill.db.jdbc.dao.HistoryDaoJdbcNoBatchUpdate;
import com.tsurugidb.benchmark.phonebill.db.postgresql.dao.DdlPostgresqlNoBatchUpdate;

public class PhoneBillDbManagerPostgresqlNoBatchUpdate extends PhoneBillDbManagerPostgresql {

	public PhoneBillDbManagerPostgresqlNoBatchUpdate(Config config, SessionHoldingType type) {
		super(config, type);
	}

	// DAOの取得

	private ContractDao contractDao;

	@Override
	public synchronized ContractDao getContractDao() {
		if (contractDao == null) {
			contractDao = new ContractDaoJdbcNoBatchUpdate(this);

		}
		return contractDao;
	}

	private HistoryDao historyDao;

	@Override
	public synchronized HistoryDao getHistoryDao() {
		if (historyDao == null) {
			historyDao = new HistoryDaoJdbcNoBatchUpdate(this);
		}
		return historyDao;
	}

	private Ddl ddl;

	@Override
	public synchronized Ddl getDdl() {
		if (ddl == null) {
			ddl = new DdlPostgresqlNoBatchUpdate(this);
		}
		return ddl;
	}

}
