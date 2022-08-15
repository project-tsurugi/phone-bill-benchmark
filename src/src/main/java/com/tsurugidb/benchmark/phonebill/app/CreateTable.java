package com.tsurugidb.benchmark.phonebill.app;

import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.dao.Ddl;

public class CreateTable extends ExecutableCommand{
	private Ddl ddl;

	public static void main(String[] args) throws Exception {
		Config config = Config.getConfig(args[0]);
		CreateTable createTable = new CreateTable();
		createTable.execute(config);
	}

	@Override
	public void execute(Config config) throws Exception {
		PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config);
		ddl = manager.getDdl();
		ddl.dropTables();
		ddl.createHistoryTable();
		ddl.createContractsTable();
		ddl.createBillingTable();
		ddl.createIndexes();
	}

	/**
	 * UT専用、UT以外での使用禁止
	 *
	 * @return ddl
	 */
	Ddl getDdlExector() {
		return ddl;
	}
}
