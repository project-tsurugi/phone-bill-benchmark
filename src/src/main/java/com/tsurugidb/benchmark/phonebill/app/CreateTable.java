package com.tsurugidb.benchmark.phonebill.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.TxLabel;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.dao.Ddl;

public class CreateTable extends ExecutableCommand{
    private static final Logger LOG = LoggerFactory.getLogger(CreateTable.class);

    public static void main(String[] args) throws Exception {
        Config config = Config.getConfig(args[0]);
        CreateTable createTable = new CreateTable();
        CrashDumper.enable();
        createTable.execute(config);
        CrashDumper.disable();
    }

    @Override
    public void execute(Config config) throws Exception {
        try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
            Ddl ddl = manager.getDdl();
            TxOption option = TxOption.ofOCC(Integer.MAX_VALUE, TxLabel.DDL);
            LOG.info("Creating tables, dropping if exist.");
            manager.execute(option, ddl::dropTables);
            manager.execute(option, ddl::createHistoryTable);
            manager.execute(option, ddl::createContractsTable);
            manager.execute(option, ddl::createBillingTable);
            manager.execute(option, ddl::createIndexes);
            LOG.info("Tables created.");
        }
    }
}
