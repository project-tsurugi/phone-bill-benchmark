package com.tsurugidb.benchmark.phonebill.online;

import java.io.IOException;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.TxLabel;
import com.tsurugidb.benchmark.phonebill.db.TxOption.Table;
import com.tsurugidb.benchmark.phonebill.db.dao.ContractDao;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * AbstractOnlineAppのスケジューラの動作確認用サンプルアプリ2.
 * <br>
 * 連続実行するケース
 *
 */
public class ExampleOnlineApp3 extends AbstractOnlineApp {
    private static final Logger LOG = LoggerFactory.getLogger(ExampleOnlineApp3.class);
    private final DateFormat DF = new SimpleDateFormat("HH:mm:ss.SSS");

    public ExampleOnlineApp3() throws SQLException, IOException {
        super(-1, Config.getConfig(), new Random());
    }

    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
    public static void main(String[] args) throws InterruptedException, SQLException, IOException {
        ExampleOnlineApp3 app = new ExampleOnlineApp3();
        ExecutorService service = Executors.newSingleThreadExecutor();
        service.submit(app);
        Thread.sleep(60 * 1000 * 2); // 2分実行して終了する。
        app.terminate();
        service.shutdown();
        service.awaitTermination(1, TimeUnit.DAYS);
    }

    @Override
    protected void atScheduleListCreated(List<Long> scheduleList) {
        for(long schedule: scheduleList) {
            LOG.info("Scheduled at {}", DF.format(new Date(schedule)));
        }
    }

    @Override
    protected void createData(ContractDao contractDao, HistoryDao historyDao) {
        LOG.info("createData called.");
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            // Nothing to do
        }
        LOG.info("createData Done.");
    }

    @Override
    protected void updateDatabase(ContractDao contractDao, HistoryDao historyDao) {
        LOG.info("updateDatabase called.");
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            // Nothing to do
        }
        LOG.info("updateDatabase Done.");
    }

    @Override
    public TxLabel getTxLabel() {
        return TxLabel.TEST;
    }

    @Override
    public Table getWritePreserveTable() {
        return Table.HISTORY;
    }

    @Override
    protected void afterCommitSuccess() {
        // Nothing to do
    }
}
