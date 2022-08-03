package com.tsurugidb.benchmark.phonebill.app;

import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.interfaces.DdlLExecutor;

public class CreateTable extends ExecutableCommand{
	public static void main(String[] args) throws Exception {
		Config.setConfigForAppConfig(false);
		Config config = Config.getConfigForAppConfig();
		CreateTable createTable = new CreateTable();
		createTable.execute(config);
	}

	@Override
	public void execute(Config config) throws Exception {
		PhoneBillDbManager manager = PhoneBillDbManager.createInstance(config);
		DdlLExecutor ddlExector = manager.getDdlLExecutor();
		ddlExector.dropTables();
		ddlExector.createHistoryTable();
		ddlExector.createContractsTable();
		ddlExector.createBillingTable();
		ddlExector.createIndexes();
	}
}
