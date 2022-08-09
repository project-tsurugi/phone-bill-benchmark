package com.tsurugidb.benchmark.phonebill.app;

import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.interfaces.DdlLExecutor;

public class CreateTable extends ExecutableCommand{
	private DdlLExecutor ddlExector;

	public static void main(String[] args) throws Exception {
		Config config = Config.getConfig(false);
		CreateTable createTable = new CreateTable();
		createTable.execute(config);
	}

	@Override
	public void execute(Config config) throws Exception {
		PhoneBillDbManager manager = config.getDbManager();
		ddlExector = manager.getDdlLExecutor();
		ddlExector.dropTables();
		ddlExector.createHistoryTable();
		ddlExector.createContractsTable();
		ddlExector.createBillingTable();
		ddlExector.createIndexes();
	}

	/**
	 * UT専用、UT以外での使用禁止
	 *
	 * @return ddlExector
	 */
	DdlLExecutor getDdlExector() {
		return ddlExector;
	}
}
