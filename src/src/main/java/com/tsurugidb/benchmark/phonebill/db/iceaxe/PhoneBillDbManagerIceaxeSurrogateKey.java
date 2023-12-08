package com.tsurugidb.benchmark.phonebill.db.iceaxe;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.dao.Ddl;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.dao.DdlIceaxeSurrogateKey;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.dao.HistoryDaoIceaxeSurrogateKey;

public class PhoneBillDbManagerIceaxeSurrogateKey extends PhoneBillDbManagerIceaxe {

	public PhoneBillDbManagerIceaxeSurrogateKey(Config config) {
		super(config);
	}


	private Ddl ddl;

	@Override
	public Ddl getDdl() {
		if (ddl == null) {
			ddl = new DdlIceaxeSurrogateKey(this);
		}
		return ddl;
	}

	private HistoryDao historyDao;

	@Override
	public HistoryDao getHistoryDao() {
		if (historyDao == null) {
			historyDao = new HistoryDaoIceaxeSurrogateKey(this);
		}
		return historyDao;
	}

}
