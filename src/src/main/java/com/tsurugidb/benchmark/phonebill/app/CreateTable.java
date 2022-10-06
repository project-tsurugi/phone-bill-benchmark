package com.tsurugidb.benchmark.phonebill.app;

import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.dao.Ddl;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;

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
			manager.execute(TgTmSetting.of(TgTxOption.ofOCC()), ddl::dropTables);
			manager.execute(TgTmSetting.of(TgTxOption.ofOCC()), ddl::createHistoryTable);
			manager.execute(TgTmSetting.of(TgTxOption.ofOCC()), ddl::createContractsTable);
			manager.execute(TgTmSetting.of(TgTxOption.ofOCC()), ddl::createBillingTable);
			manager.execute(TgTmSetting.of(TgTxOption.ofOCC()), ddl::createIndexes);
		}
	}
}
