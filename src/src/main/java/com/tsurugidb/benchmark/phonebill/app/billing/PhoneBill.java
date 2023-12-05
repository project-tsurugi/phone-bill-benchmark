package com.tsurugidb.benchmark.phonebill.app.billing;

import java.io.IOException;
import java.sql.Date;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.CrashDumper;
import com.tsurugidb.benchmark.phonebill.app.ExecutableCommand;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager.SessionHoldingType;
import com.tsurugidb.benchmark.phonebill.db.TxLabel;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.TxOption.Table;
import com.tsurugidb.benchmark.phonebill.db.dao.BillingDao;
import com.tsurugidb.benchmark.phonebill.db.dao.ContractDao;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract.Key;
import com.tsurugidb.benchmark.phonebill.db.jdbc.Duration;
import com.tsurugidb.benchmark.phonebill.online.AbstractOnlineApp;
import com.tsurugidb.benchmark.phonebill.online.HistoryInsertApp;
import com.tsurugidb.benchmark.phonebill.online.HistoryUpdateApp;
import com.tsurugidb.benchmark.phonebill.online.MasterDeleteInsertApp;
import com.tsurugidb.benchmark.phonebill.online.MasterUpdateApp;
import com.tsurugidb.benchmark.phonebill.online.RandomKeySelector;
import com.tsurugidb.benchmark.phonebill.testdata.ActiveBlockNumberHolder;
import com.tsurugidb.benchmark.phonebill.testdata.ContractBlockInfoAccessor;
import com.tsurugidb.benchmark.phonebill.testdata.DbContractBlockInfoInitializer;
import com.tsurugidb.benchmark.phonebill.testdata.SingleProcessContractBlockManager;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class PhoneBill extends ExecutableCommand {
    private static final Logger LOG = LoggerFactory.getLogger(PhoneBill.class);

    private long elapsedTime = 0; // バッチの処理時間
    private CalculationTargetQueue queue;
    private String finalMessage;
    private AtomicBoolean abortRequested = new AtomicBoolean(false);
    private AtomicInteger tryCounter = new AtomicInteger(0);
    private AtomicInteger abortCounter = new AtomicInteger(0);


    Config config; // UTからConfigを書き換え可能にするためにパッケージプライベートにしている

    public static void main(String[] args) throws Exception {
        Config config = Config.getConfig(args);
        PhoneBill phoneBill = new PhoneBill();
        CrashDumper.enable();
        phoneBill.execute(config);
        CrashDumper.disable();
    }

    @Override
    public void execute(Config config) throws Exception {
        this.config = config.clone();
        DbContractBlockInfoInitializer initializer = new DbContractBlockInfoInitializer(config);
        ContractBlockInfoAccessor accessor = new SingleProcessContractBlockManager(initializer);

        List<AbstractOnlineApp> list = createOnlineApps(config, accessor);
        if (list.isEmpty() && config.onlineOnly) {
            LOG.warn("Specified 'onlineOnly', but no online application to run, exiting immediately.");
            return;
        }
        final ExecutorService service = list.isEmpty() ? null : Executors.newFixedThreadPool(list.size());

        Timer timer = new Timer(false);
        try {
            PhoneBillDbManager.initCounter();
            // オンラインアプリを実行する
            list.stream().forEach(task -> service.submit(task));

            // 指定の実行時間になったら停止するためのタイマーをセット
            CountDownLatch latch = new CountDownLatch(1);
            if (config.execTimeLimitSecs > 0) {
                TimerTask timerTask = new TimerTask() {
                    @Override
                    public void run() {
                        timer.cancel();
                        latch.countDown();
                        abort();
                    }
                };
                timer.schedule(timerTask, config.execTimeLimitSecs * 1000L);
            }

            // バッチの実行
            if (config.onlineOnly) {
                // online onlyが指定された場合は、タイマーが呼び出されるまで待つ
                latch.await();
            } else {
                Duration d = toDuration(config.targetMonth);
                doCalc(d.getStatDate(), d.getEndDate());
            }
        } finally {
            timer.cancel();
            // オンラインアプリを終了する
            list.stream().forEach(task -> task.terminate());
            if (service != null) {
                service.shutdown();
                service.awaitTermination(5, TimeUnit.MINUTES);
            }
            // 終了していないオンラインアプリがある場合は異常終了する。
            boolean allTerminated = list.stream().allMatch(task -> task.getTerminated());
            if (!allTerminated) {
                System.exit(1);
            }
            LOG.debug("Counter infos: \n---\n{}---", PhoneBillDbManager.createCounterReport());
        }
    }



    /**
     * Configに従ってオンラインアプリのインスタンスを生成する
     *
     * @return オンラインアプリのインスタンスのリスト
     * @throws IOException
     */
    @SuppressFBWarnings(value={"DMI_RANDOM_USED_ONLY_ONCE"})
    public static List<AbstractOnlineApp> createOnlineApps(Config config, ContractBlockInfoAccessor accessor)
            throws IOException {
        if (!config.hasOnlineApp()) {
            return Collections.emptyList();
        }

        Random random = new Random(config.randomSeed);
        ActiveBlockNumberHolder blockHolder = accessor.getActiveBlockInfo();
        if (blockHolder.getNumberOfActiveBlacks() < 1) {
            throw new IllegalStateException("Insufficient test data, create test data first.");
        }

        RandomKeySelector<Key> keySelector;
        try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
            List<Key> keys = manager.execute(TxOption.ofRTX(3, TxLabel.BATCH_INITIALIZE), () -> {
                return manager.getContractDao().getAllPrimaryKeys();
            });
            keySelector = new RandomKeySelector<>(keys, random, config.onlineAppRandomAtLeastOnceRate,
                    config.onlineAppRandomCoverRate);
        }


        List<AbstractOnlineApp> list = new ArrayList<AbstractOnlineApp>();
        if (config.historyInsertThreadCount > 0 && config.historyInsertTransactionPerMin != 0) {
            list.addAll(HistoryInsertApp.createHistoryInsertApps(config, new Random(random.nextInt()), accessor,
                    config.historyInsertThreadCount));
        }
        if (config.historyUpdateThreadCount > 0 && config.historyUpdateRecordsPerMin != 0) {
            for (int i = 0; i < config.historyUpdateThreadCount; i++) {
                AbstractOnlineApp task = new HistoryUpdateApp(config, new Random(random.nextInt()), keySelector);
                task.setName(i);
                list.add(task);
            }
        }
        if (config.masterDeleteInsertThreadCount > 0 && config.masterDeleteInsertRecordsPerMin != 0) {
            for (int i = 0; i < config.masterDeleteInsertThreadCount; i++) {
                AbstractOnlineApp task = new MasterDeleteInsertApp(config, new Random(random.nextInt()), keySelector);
                task.setName(i);
                list.add(task);
            }
        }
        if (config.masterUpdateThreadCount > 0 && config.masterUpdateRecordsPerMin != 0) {
            for (int i = 0; i < config.masterUpdateThreadCount; i++) {
                AbstractOnlineApp task = new MasterUpdateApp(config, new Random(random.nextInt()), keySelector);
                task.setName(i);
                list.add(task);
            }
        }
        return list;
    }


    /**
     * 指定の日付の一日から月の最終日までのDurationを作成する
     *
     * @param date
     * @return
     */
    public static Duration toDuration(Date date) {
        LocalDate localDate = date.toLocalDate();
        Date start = Date.valueOf(localDate.withDayOfMonth(1));
        Date end = Date.valueOf(localDate.withDayOfMonth(1).plusMonths(1).minusDays(1));
        return new Duration(start, end);
    }


    /**
     * 料金計算のメイン処理
     *
     * @param config
     * @param start
     * @param end
     * @throws Exception
     */
    void doCalc(Date start, Date end) throws Exception {
        abortRequested.set(false);
        LOG.info("Phone bill batch started.");
        String batchExecId = UUID.randomUUID().toString();
        int threadCount = config.threadCount;

        ExecutorService service = null;
        Set<Future<Exception>> futures = new HashSet<>(threadCount);

        long startTime = System.currentTimeMillis();
        PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config,
                SessionHoldingType.INSTANCE_FIELD);
        List<PhoneBillDbManager> managers = new ArrayList<>();
        managers.add(manager);
        try {
            BillingDao billingDao = manager.getBillingDao();
            ContractDao contractDao = manager.getContractDao();

            // Billingテーブルの計算対象月のレコードを削除する
            manager.execute(TxOption.ofLTX(0, TxLabel.BATCH_INITIALIZE, Table.BILLING), () -> {
                billingDao.delete(start);
            });

            // 計算対象の契約を取りだし、キューに入れる
            List<Contract> list = manager.execute(TxOption.ofRTX(Integer.MAX_VALUE, TxLabel.BATCH_INITIALIZE), () -> {
                return contractDao.getContracts(start, end);
            });
            ArrayList<CalculationTarget> targets = new ArrayList<>(list.size());
            for (Contract contract : list) {
                LOG.debug(contract.toString());

                CallChargeCalculator callChargeCalculator = new SimpleCallChargeCalculator();
                BillingCalculator billingCalculator = new SimpleBillingCalculator();
                CalculationTarget target = new CalculationTarget(contract, billingCalculator, callChargeCalculator,
                        start, end, false);
                targets.add(target);
            }
            queue = new CalculationTargetQueue(targets);

            // 契約毎の計算を行うスレッドを生成する
            service = Executors.newFixedThreadPool(threadCount);
            for (int i = 0; i < threadCount; i++) {
                PhoneBillDbManager managerForTask;
                if (config.sharedConnection) {
                    managerForTask = manager;
                } else {
                    managerForTask = PhoneBillDbManager.createPhoneBillDbManager(config,
                            SessionHoldingType.INSTANCE_FIELD);
                    managers.add(managerForTask);
                }
                CalculationTask task = new CalculationTask(queue, managerForTask, config, batchExecId, abortRequested, tryCounter, abortCounter);
                futures.add(service.submit(task));
            }
        } catch (RuntimeException e) {
            abortRequested.set(true);
            throw e;

        } finally {
            if (service != null && !service.isTerminated()) {
                service.shutdown();
            }
            cleanup(futures, managers);
        }
        elapsedTime = System.currentTimeMillis() - startTime;
        String format = "Billings calculated in %,.3f sec ";
        finalMessage = String.format(format, elapsedTime / 1000d);
        LOG.info(finalMessage);
    }

    public String getStatus() {
        return queue == null ? "Initializing": queue.getStatus();
    }



    /**
     * @param conn
     * @param futures
     * @param managers
     * @param abortRequested
     * @throws Exception
     */
    private void cleanup(Set<Future<Exception>> futures, List<PhoneBillDbManager> managers) throws Exception {
        Exception cause = null;
        while (!futures.isEmpty()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                LOG.debug("InterruptedException caught and waiting service shutdown.", e);
            }
            Iterator<Future<Exception>> it = futures.iterator();
            while (it.hasNext()) {
                Exception e = null;
                {
                    Future<Exception> future = it.next();
                    if (future.isDone()) {
                        it.remove();
                        try {
                            if (!future.isCancelled()) {
                                e = future.get(0, TimeUnit.SECONDS);
                            }
                        } catch (InterruptedException e1) {
                            continue;
                        } catch (ExecutionException e1) {
                            e = e1;
                        } catch (TimeoutException e1) {
                            continue;
                        }
                    }
                }
                if (e != null && cause == null) {
                    cause = e;
                    abortRequested.set(true);
                }
            }
        }
        managers.stream().forEach(m->m.close());
        if (cause != null) {
            LOG.error("Phone bill batch aborting by exception.");
            throw cause;
        }
    }

    /**
     * バッチの処理時間を取得する
     *
     * @return elapsedTime
     */
    public long getElapsedTime() {
        return elapsedTime;
    }

    /**
     * 終了時のメッセージを返す
     *
     * @return
     */
    public String getFinalMessage() {
        return finalMessage;
    }

    /**
     * バッチを中断する
     */
    public void abort() {
        abortRequested.set(true);
    }

    /**
     * 電話料金計算の試行回数を返す。
     * <p>
     * 1電話番号の処理を1回とカウントします。トランザクションがリトライ可能な例外でabortすると、リトライが行われ試行回数がインクリメントされます。
     * 特定の電話番号の処理がn回リトライされると、試行回数はn+1としてカウントされます。
     *
     * @return 試行回数。
     */
    public int getTryCount() {
        return tryCounter.get();
    }

    /**
     * トランザクションがabortした回数を返します。
     * <p>
     * 正確にはTransactionが失敗した回数を返します。状況により、abortした回数と異なる値が返ることがあります。
     *
     * @return
     */
    public int getAbortCount() {
        return abortCounter.get();
    }

    /**
     * abortがリクエストされたときtrueを返す(UT用)
     *
     * @return
     */
    protected boolean getAbortRequested() {
        return abortRequested.get();
    }
}
