package com.tsurugidb.benchmark.phonebill.app;

import com.tsurugidb.benchmark.phonebill.db.interfaces.DdlLExecutor;
import com.tsurugidb.benchmark.phonebill.db.jdbc.Session;

public class CreateTable extends ExecutableCommand{
	public static void main(String[] args) throws Exception {
		Config config = Config.getConfig(args);
		CreateTable createTable = new CreateTable();
		createTable.execute(config);
	}

	@Override
	public void execute(Config config) throws Exception {
		try (Session session = Session.getSession(config)) {
			DdlLExecutor ddlExector = DdlLExecutor.getInstance(config);
			ddlExector.dropTables(session);
			ddlExector.createHistoryTable(session);
			ddlExector.createContractsTable(session);
			ddlExector.createBillingTable(session);
			ddlExector.createIndexes(session);
		}
	}
}
