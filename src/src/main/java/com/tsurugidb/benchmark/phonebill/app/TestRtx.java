package com.tsurugidb.benchmark.phonebill.app;

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.billing.CalculationTarget;
import com.tsurugidb.benchmark.phonebill.app.billing.PhoneBill;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.TxLabel;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.dao.ContractDao;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.benchmark.phonebill.online.AbstractOnlineApp;
import com.tsurugidb.benchmark.phonebill.online.TxStatistics;
import com.tsurugidb.benchmark.phonebill.testdata.ContractBlockInfoAccessor;
import com.tsurugidb.benchmark.phonebill.testdata.DbContractBlockInfoInitializer;
import com.tsurugidb.benchmark.phonebill.testdata.SingleProcessContractBlockManager;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;

public class TestRtx extends ExecutableCommand {
    private static final Logger LOG = LoggerFactory.getLogger(TestRtx.class);

    private static final TxOption RTX = TxOption.ofRTX(0, TxLabel.TEST);

    private static final Date START_TIME = DateUtils.toDate("2000-01-05");
    private static final Date END_TIME = DateUtils.toDate("2023-02-03");

    private static AtomicInteger executionRemaining;
    private static List<CalculationTarget> targets = new ArrayList<>(100);


    @Override
    public void execute(Config config) throws Exception {
        initTargets(config);
        DbContractBlockInfoInitializer initializer = new DbContractBlockInfoInitializer(config);
        ContractBlockInfoAccessor accessor = new SingleProcessContractBlockManager(initializer);

        List<AbstractOnlineApp> list = PhoneBill.createOnlineApps(config, accessor);
        if (list.isEmpty() && config.onlineOnly) {
            LOG.warn("Specified 'onlineOnly', but no online application to run, exiting immediately.");
            return;
        }
        final ExecutorService service = list.isEmpty() ? null : Executors.newFixedThreadPool(list.size() * 2);

        try {
            PhoneBillDbManager.initCounter();
            TxStatistics.clear();
            // オンラインアプリを実行する
            list.parallelStream().forEach(task -> service.submit(task));

            // オンラインアプリと同じ数だけTestRtxTaskを実行する。
            executionRemaining = new AtomicInteger(1000000);
            long start = System.currentTimeMillis();
            List<Future<?>> futures = new ArrayList<>(list.size());
            for(int i = 0; i < list.size(); i++) {
                Future<?> f= service.submit(new TestRtxTask(config));
                futures.add(f);
            }

            // すべてのTestRtxTaskが終了するのを待つ。
            for (Future<?> f : futures) {
                f.get();
            }
            long dedicatedTimeMills = System.currentTimeMillis() - start;
            TxStatistics.setDedicatedTimeMills(dedicatedTimeMills);

            LOG.info("All TestRtxTask tasks have been completed.");

        } finally {
            // オンラインアプリを終了する
            list.stream().forEach(task -> task.terminate());
            if (service != null) {
                service.shutdown();
                service.awaitTermination(5, TimeUnit.MINUTES);
            }
            MultipleExecute.writeOnlineAppReport(config);
            LOG.debug("Counter infos: \n---\n{}---", PhoneBillDbManager.createCounterReport());
        }
    }

    private void initTargets(Config config) {
        try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
            HistoryDao hDao = manager.getHistoryDao();
            ContractDao cDdao = manager.getContractDao();
            List<Contract> contracts = manager.execute(RTX, () -> {
                return cDdao.getContracts();
            });
            // 対応する履歴が存在する契約を100抽出する。
            for (Contract c : contracts) {
                CalculationTarget ct = new CalculationTarget(c, null, null, START_TIME, END_TIME, false);
                List<History> list = manager.execute(RTX, () -> {
                    return hDao.getHistories(ct);
                });
                if (list.size() == 0) {
                    continue;
                }
                targets.add(ct);
                if (targets.size() >= 100) {
                    break;
                }
            }
        }

    }


    private static class TestRtxTask implements Runnable {
        private PhoneBillDbManager manager;
        private HistoryDao dao;

        public TestRtxTask(Config config) {
            manager = PhoneBillDbManager.createPhoneBillDbManager(config);
            dao = manager.getHistoryDao();
        }


        @Override
        public void run() {
            try {
                exec();

            } finally {
                manager.close();
            }
        }

        private void exec() {
            for (;;) {
                int n = executionRemaining.decrementAndGet();
                if (n < 0) {
                    return;
                }
                CalculationTarget ct = targets.get(n % targets.size());
                long start = System.nanoTime();
                List<History> list = manager.execute(RTX, () -> {
                    return dao.getHistories(ct);
                });
                long time = System.nanoTime() - start;
                String fmt = "exec remain = %d , phone_number = %s , count = %d , time[nano] = %d , nao/count = %d ";
                LOG.debug(String.format(fmt, n, ct.getContract().getPhoneNumber(), list.size(), time,
                        time / list.size()));
            }
        }
    }
}
