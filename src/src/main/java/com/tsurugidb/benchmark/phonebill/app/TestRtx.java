package com.tsurugidb.benchmark.phonebill.app;

import java.sql.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.billing.CalculationTarget;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.TxLabel;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.dao.ContractDao;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;

public class TestRtx extends ExecutableCommand {
    private static final Logger LOG = LoggerFactory.getLogger(TestRtx.class);

    private static final TxOption RTX = TxOption.ofRTX(0, TxLabel.TEST);
    private static final TxOption LTX = TxOption.ofLTX(0, TxLabel.TEST);
    private static final Date START_TIME = DateUtils.toDate("2000-01-05");
    private static final Date END_TIME = DateUtils.toDate("2023-02-03");

    private Config config;

    @Override
    public void execute(Config config) throws Exception {
        this.config = config;

        readHistoryFull(RTX, 10);
        readHistoryFull(LTX, 10);
        readMany(LTX, 1);
        readMany(RTX, 1);


    }

    private void readMany(TxOption txOption, int count) {
        try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
            HistoryDao hDao = manager.getHistoryDao();
            ContractDao cDdao = manager.getContractDao();
            int nExec = 0;
            int nRecords = 0;
            for (int i = 0; i < count; i++) {
                long start = System.nanoTime();
                List<Contract> contracts = manager.execute(txOption, () -> {
                    return cDdao.getContracts();
                });
                for (Contract c: contracts) {
                    CalculationTarget ct = new CalculationTarget(c, null, null, START_TIME, END_TIME, false);
                    List<History> list = manager.execute(txOption, () -> {
                        return hDao.getHistories(ct);
                    });
                    nExec++;
                    nRecords+=list.size();
                    LOG.debug("size = {}" + list.size());
                }
                long time = System.nanoTime() - start;
                LOG.info("{}: exec {} select and read {} histories in {} ms.", txOption, nExec, nRecords, String.format("%,.3f", time / 1e6d));
            }
        }

    }

    private void readHistoryFull(TxOption txOption, int count) {
        try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
            HistoryDao dao = manager.getHistoryDao();
            for (int i = 0; i < count; i++) {
                long start = System.nanoTime();
                List<History> list = manager.execute(txOption, () -> {
                    return dao.getHistories();
                });
                long time = System.nanoTime() - start;
                LOG.info("{}: Read {} histories in {} ms.", txOption, list.size(), String.format("%,.3f", time / 1e6d));
            }
        }
    }

}
