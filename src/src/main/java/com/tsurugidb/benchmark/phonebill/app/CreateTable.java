package com.tsurugidb.benchmark.phonebill.app;

import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
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
		try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
			ddl = manager.getDdl();
			if (config.usePreparedTables) {
				manager.execute(TxOption.ofLTX(0, "DropTable", "history", "contracts", "billing"),
						() -> ddl.truncateTable("history"));
			}
			TxOption option = TxOption.ofOCC(0, "CreateTable");
			manager.execute(option, ddl::dropTables);
			manager.execute(option, ddl::createHistoryTable);
			manager.execute(option, ddl::createContractsTable);
			manager.execute(option, ddl::createBillingTable);
			manager.execute(option, ddl::createIndexes);
		}
	}
}
