package com.tsurugidb.benchmark.phonebill.app.billing;

import java.sql.Date;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.Config.TransactionScope;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.RetryOverRuntimeException;
import com.tsurugidb.benchmark.phonebill.db.TxLabel;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.TxOption.Table;
import com.tsurugidb.benchmark.phonebill.db.dao.BillingDao;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;
import com.tsurugidb.benchmark.phonebill.db.entity.Billing;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.History;

public class CalculationTask implements Callable<Exception> {
    private static final Logger LOG = LoggerFactory.getLogger(CalculationTask.class);
    private static final Logger CMT_LOG = LoggerFactory.getLogger(CalculationTask.class.getName() + "-cmt");
    private Config config;
    private String batchExecId;
    private AtomicBoolean abortRequested;
    private AtomicInteger tryCounter;
    private AtomicInteger abortCounter;
    private int nCalculated = 0;
    private TxOption txOption = null;

    Calculator calculator;


    /**
     * 計算対象が格納されているQueue
     */
    private CalculationTargetQueue queue;

    // DBManagerとDAO
    private PhoneBillDbManager manager;
    private BillingDao billingDao;
    private HistoryDao historyDao;

    /**
     * コンストラクタ
     *
     * @param queue
     * @param conn
     */
    public CalculationTask(CalculationTargetQueue queue, PhoneBillDbManager manager,  Config config, String batchExecId,
            AtomicBoolean abortRequested, AtomicInteger tryCounter, AtomicInteger abortCounter) {
        this.queue = queue;
        this.config = config;
        this.batchExecId = batchExecId;
        this.abortRequested = abortRequested;
        this.tryCounter = tryCounter;
        this.abortCounter = abortCounter;
        this.manager = manager;
        billingDao = manager.getBillingDao();
        historyDao = manager.getHistoryDao();
        calculator = new CalculatorImpl();
        switch (config.transactionOption) {
        case OCC:
            txOption = TxOption.ofOCC(0, TxLabel.BATCH_MAIN);
            break;
        case LTX:
            txOption = TxOption.ofLTX(0, TxLabel.BATCH_MAIN, Table.HISTORY, Table.BILLING);
            break;
        }
    }

    @Override
    public Exception call() throws Exception {
        LOG.info("Calculation task started.");
        Timer timer = new Timer(txOption);

        if (config.transactionScope == TransactionScope.CONTRACT) {
            while (continueLoop()) {
                CalculationTarget target = queue.take();
                if (target == null) {
                    continue;
                }
                String phoneNumber = target.getContract().getPhoneNumber();
                LOG.debug(queue.getStatus());
                TransactionId tid = new TransactionId();
                AtomicInteger records = new AtomicInteger(0);
                try {
                    manager.execute(txOption, () -> {
                        tryCounter.incrementAndGet();
                        tid.set(manager.getTransactionId());
                        timer.setStartTx(tid, phoneNumber);
                        records.addAndGet(calculator.doCalc(target));
                        timer.setStartCommit(phoneNumber);
                    });
                    queue.success(target);
                    timer.setEndCommit(phoneNumber, records.get());
                    timer.setAbort(phoneNumber, new RuntimeException("dummy"));
                    nCalculated++;
                } catch (RuntimeException e) {
                    abortCounter.incrementAndGet();
                    queue.revert(target);
                    PhoneBillDbManager.addRetringExceptions(e);
                    timer.setAbort(phoneNumber, e);
                    if (!(e instanceof RetryOverRuntimeException)) {
                        LOG.debug("Calculation task aborted.", e);
                        return e;
                    }
                }
            }
        } else {
            while (continueLoop()) {
                List<CalculationTarget> list = new ArrayList<>();
                CalculationTarget firstTarget = queue.take();
                if (firstTarget == null) {
                    continue;
                }
                LOG.debug(queue.getStatus());
                list.add(firstTarget);
                TransactionId tid = new TransactionId();
                try {
                    AtomicInteger records = new AtomicInteger(0);
                    manager.execute(txOption, () -> {
                        tid.set(manager.getTransactionId());
                        timer.setStartTx(tid, "-");
                        tryCounter.incrementAndGet();
                        calculator.doCalc(firstTarget);
                        while (abortRequested.get() == false) {
                            CalculationTarget target;
                            target = queue.poll();
                            if (target == null) {
                                break;
                            }
                            LOG.debug(queue.getStatus());
                            list.add(target);
                            tryCounter.incrementAndGet();
                            records.addAndGet(calculator.doCalc(target));
                        }
                        timer.setStartCommit("-");
                    });
                    nCalculated += list.size();
                    timer.setEndCommit("-", records.get());
                    queue.success(list);
                } catch (RuntimeException e) {
                    abortCounter.incrementAndGet();
                    PhoneBillDbManager.addRetringExceptions(e);
                    // 処理対象をキューに戻す
                    queue.revert(list);
                    timer.setAbort("-", e);
                    if (!(e instanceof RetryOverRuntimeException)) {
                        LOG.error("Calculation task aborting by exception.", e);
                        return e;
                    }
                }
            }
        }
        return null;
    }

    private boolean continueLoop() {
        if (abortRequested.get() == true) {
            LOG.info("Calculation task finished by abort rquest, number of calculated contracts = {}.", nCalculated);
            return false;
        }
        if (queue.finished()) {
            LOG.info("Calculation task finished normally, number of calculated contracts = {}.", nCalculated);
            return false;
        }
        return true;
    }

    static String getPhoneNumbers(List<CalculationTarget> list) {
        return String.join(",", list.stream().map(t -> t.getContract().getPhoneNumber()).collect(Collectors.toList()));
    }


    /**
     * Billingテーブルを更新する
     *
     * @param contract
     * @param billingCalculator
     * @param targetMonth
     */
    private void updateBilling(Contract contract, BillingCalculator billingCalculator, Date targetMonth) {
        LOG.debug(
                "Inserting to billing table: phone_number = {}, target_month = {}"
                        + ", basic_charge = {}, metered_charge = {}, billing_amount = {}, batch_exec_id = {} ",
                contract.getPhoneNumber(), targetMonth, billingCalculator.getBasicCharge(),
                billingCalculator.getMeteredCharge(), billingCalculator.getBillingAmount(), batchExecId);
        Billing billing = new Billing();
        billing.setPhoneNumber(contract.getPhoneNumber());
        billing.setTargetMonth(targetMonth);
        billing.setBasicCharge(billingCalculator.getBasicCharge());
        billing.setMeteredCharge(billingCalculator.getMeteredCharge());
        billing.setBillingAmount(billingCalculator.getBillingAmount());
        billing.setBatchExecId(batchExecId);
        billingDao.insert(billing);
    }


    // call()のUTのために、doCalcメソッドを置き換え可能にする。

    protected void setCalculator(Calculator calculator) {
        this.calculator = calculator;
    }


    protected static interface Calculator {
        /**
         * 料金計算のメインロジック
         *
         * @param target
         * @return 更新したレコード数
         */
        int doCalc(CalculationTarget target);
    }

    protected class CalculatorImpl implements Calculator {
        @Override
        public int doCalc(CalculationTarget target) {
            LOG.debug("Start calculation for  contract: {}.", target.getContract());

            Contract contract = target.getContract();

            List<History> histories = historyDao.getHistories(target);
            LOG.info("calculation: phone_number = {}, count = {}", target.getContract().getPhoneNumber(), histories.size());

            CallChargeCalculator callChargeCalculator = target.getCallChargeCalculator();
            BillingCalculator billingCalculator = target.getBillingCalculator();
            billingCalculator.init();
            for (History h : histories) {
                if (h.getTimeSecs() < 0) {
                    throw new RuntimeException("Negative time: " + h.getTimeSecs());
                }
                h.setCharge(callChargeCalculator.calc(h.getTimeSecs()));
                billingCalculator.addCallCharge(h.getCharge());
            }
            historyDao.batchUpdate(histories);
            updateBilling(contract, billingCalculator, target.getStart());
            return histories.size() + 1; // +1はupdateBillingの分
        }
    }

    static class TransactionId {
        private String tid ="none";

        void set(String tid) {
            this.tid = tid;
        }

        @Override
        public String toString() {
            return tid;
        }
    }

    /**
     * txOptionを取得する(UT用)
     *
     * @return txOption
     */
    TxOption getTxOption() {
        return txOption;
    }

    /**
     * TXの処理時間をログに記録するためのタイマ
     */
    static class Timer {
        private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

        private TransactionId tid;
        private TxOption txOption;

        private Instant startTx = null;
        private Instant startCommit = null;
        private Instant endTx = null;

        public Timer(TxOption txOption) {
            this.txOption = txOption;
        }

        public void setStartTx(TransactionId tid, String phoneNumber) {
            this.tid = tid;
            LOG.debug("Transaction starting, tid = {}, txOption = {}, key = {}", tid, txOption, phoneNumber);
            startTx = Instant.now();
            startCommit = null;
            endTx = null;
        }

        public void setStartCommit(String phoneNumber) {
            CMT_LOG.debug("Transaction committing, tid = {}, txOption = {}, key = {}", tid, txOption, phoneNumber);
            startCommit = Instant.now();
        }

        public void setEndCommit(String phoneNumber, int records) {
            CMT_LOG.debug("Transaction completed, tid = {}, txOption = {}, key = {}, update/insert records = {}", tid,
                    txOption, phoneNumber, records);
            if (startTx == null || startCommit == null) {
                return;
            }
            endTx = Instant.now();
            Duration d1 = Duration.between(startTx, startCommit);
            Duration d2 = Duration.between(startCommit, endTx);
            LOG.debug(
                    "TIME INFO: tx start timestamp = {}, tid = {}, update/insert records = {}, exec time = {}, commit time = {}",
                    TIME_FMT.format(LocalDateTime.ofInstant(startTx, ZoneId.systemDefault())), tid, records,
                    d1.toNanos() / 1000, // Durationをマイクロ秒で表示
                    d2.toNanos() / 1000); // Durationをマイクロ秒で表示
        }

        public void setAbort(String phoneNumber, RuntimeException e) {
            CMT_LOG.debug("Transaction aborted, tid = {}, txOption = {}, key = {}, exception = {}", tid, txOption, phoneNumber, e.getMessage());
            if (startTx == null) {
                return;
            }
            endTx = Instant.now();
            if (startCommit == null) {
                Duration d1 = Duration.between(startTx, endTx);
                LOG.debug("TIME INFO: tid = {}, exec to abort time = {}, commit time = {}", tid,
                        d1.toSeconds() * 1000 * 1000 + d1.toNanos() / 1000, // Durationをマイクロ秒で表示
                        "-");

            } else {
                Duration d1 = Duration.between(startTx, startCommit);
                Duration d2 = Duration.between(startCommit, endTx);
                LOG.debug("TIME INFO: tid = {}, exec time = {}, commit to abort time = {}", tid,
                        d1.toNanos() / 1000, // Durationをマイクロ秒で表示
                        d2.toNanos() / 1000); // Durationをマイクロ秒で表示

            }
        }
    }
}
