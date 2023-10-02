package com.tsurugidb.benchmark.phonebill.app.billing;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.AbstractJdbcTestCase;
import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.Config.TransactionOption;
import com.tsurugidb.benchmark.phonebill.app.Config.TransactionScope;
import com.tsurugidb.benchmark.phonebill.app.CreateTable;
import com.tsurugidb.benchmark.phonebill.app.billing.CalculationTask.Calculator;
import com.tsurugidb.benchmark.phonebill.app.billing.CalculationTask.Timer;
import com.tsurugidb.benchmark.phonebill.app.billing.CalculationTask.TransactionId;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager.CounterName;
import com.tsurugidb.benchmark.phonebill.db.RetryOverRuntimeException;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.entity.Billing;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;

class CalculationTaskTest extends AbstractJdbcTestCase {

    @Test
    final void testDoCalc() throws Exception {
        // 初期化
        Config config = Config.getConfig();
        new CreateTable().execute(config);
        PhoneBill phoneBill = new PhoneBill();
        phoneBill.config = config;

        CallChargeCalculator callChargeCalculator = new SimpleCallChargeCalculator();
        BillingCalculator billingCalculator = new SimpleBillingCalculator();

        CalculationTask task = new CalculationTask(null, getManager(), config, "", new AtomicBoolean(false),
                new AtomicInteger(0), new AtomicInteger(0));


        // 契約マスタにテストデータをセット
        insertToContracts("Phone-0001", "2010-01-01", null, "Simple");
        insertToContracts("Phone-0003", "2010-01-01", "2020-11-01", "Simple");
        insertToContracts("Phone-0004", "2020-11-30", null, "Simple");
        insertToContracts("Phone-0005", "2020-11-30", "2021-01-10", "Simple");
        insertToContracts("Phone-0008", "2020-01-21", null, "Simple");


        // 通話履歴がない状態での料金計算
        truncateTable("billing");
        for(Contract c: getContracts()) {
            CalculationTarget target = new CalculationTarget(c, billingCalculator, callChargeCalculator,
                    DateUtils.toDate("2020-11-01"), DateUtils.toDate("2020-11-30"), false);
            task.calculator.doCalc(target);
        }
        List<Billing> billings = getBillings();
        assertEquals(5, billings.size());
        assertEquals(Billing.create("Phone-0001", "2020-11-01", 3000, 0, 3000, null), billings.get(0));
        assertEquals(Billing.create("Phone-0003", "2020-11-01", 3000, 0, 3000, null), billings.get(1));
        assertEquals(Billing.create("Phone-0004", "2020-11-01", 3000, 0, 3000, null), billings.get(2));
        assertEquals(Billing.create("Phone-0005", "2020-11-01", 3000, 0, 3000, null), billings.get(3));
        assertEquals(Billing.create("Phone-0008", "2020-11-01", 3000, 0, 3000, null), billings.get(4));

        // 通話履歴ありの場合
        insertToHistory("Phone-0001", "Phone-0008", "C", "2020-10-31 23:59:59.999", 30, 0);		// 計算対象年月外
        insertToHistory("Phone-0001", "Phone-0008", "C", "2020-11-01 00:00:00.000", 30, 0);  	// 計算対象
        insertToHistory("Phone-0001", "Phone-0008", "C", "2020-11-15 12:12:12.000", 90, 1); 	 	// 削除フラグ
        insertToHistory("Phone-0001", "Phone-0008", "C", "2020-11-30 23:59:59.999", 90, 0);  	// 計算対象
        insertToHistory("Phone-0001", "Phone-0008", "C", "2020-12-01 00:00:00.000", 30, 0);  	// 計算対象年月外
        insertToHistory("Phone-0005", "Phone-0001", "C", "2020-11-10 00:00:00.000", 250, 0);  	// 計算対象
        insertToHistory("Phone-0005", "Phone-0008", "R", "2020-11-30 00:00:00.000", 30, 0);  	// 計算対象(受信者負担)


        truncateTable("billing");
        for(Contract c: getContracts()) {
            CalculationTarget target = new CalculationTarget(c, billingCalculator, callChargeCalculator,
                    DateUtils.toDate("2020-11-01"), DateUtils.toDate("2020-11-30"), false);
            task.calculator.doCalc(target);
        }

        billings = getBillings();
        assertEquals(5, billings.size());
        assertEquals(Billing.create("Phone-0001", "2020-11-01", 3000, 30, 3000, null), billings.get(0));
        assertEquals(Billing.create("Phone-0003", "2020-11-01", 3000, 0, 3000, null), billings.get(1));
        assertEquals(Billing.create("Phone-0004", "2020-11-01", 3000, 0, 3000, null), billings.get(2));
        assertEquals(Billing.create("Phone-0005", "2020-11-01", 3000, 50, 3000, null), billings.get(3));
        assertEquals(Billing.create("Phone-0008", "2020-11-01", 3000, 10, 3000, null), billings.get(4));
        var histories = getHistories();
        assertEquals(7, histories.size());
        assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-10-31 23:59:59.999", 30, null, 0), histories.get(0));
        assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-11-01 00:00:00.000", 30, 10, 0), histories.get(1));
        assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-11-15 12:12:12.000", 90, null, 1), histories.get(2));
        assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-11-30 23:59:59.999", 90, 20, 0), histories.get(3));
        assertEquals(toHistory("Phone-0001", "Phone-0008", "C", "2020-12-01 00:00:00.000", 30, null, 0), histories.get(4));
        assertEquals(toHistory("Phone-0005", "Phone-0001", "C", "2020-11-10 00:00:00.000", 250, 50, 0), histories.get(5));
        assertEquals(toHistory("Phone-0005", "Phone-0008", "R", "2020-11-30 00:00:00.000", 30, 10, 0), histories.get(6));


        // 通話時間が負の値の履歴によりエラーになるケース
        insertToHistory("Phone-0005", "Phone-0008", "R", "2020-11-29 00:00:00.000", -300, 0);  	// 計算対象(受信者負担)
        truncateTable("billing");
        for (Contract c : getContracts()) {
            CalculationTarget target = new CalculationTarget(c, billingCalculator, callChargeCalculator,
                    DateUtils.toDate("2020-11-01"), DateUtils.toDate("2020-11-30"), false);
            System.out.println(c);
            if (c.getPhoneNumber().equals("Phone-0008")) {
                Exception e = assertThrows(RuntimeException.class, () -> task.calculator.doCalc(target));
                assertEquals("Negative time: -300", e.getMessage());
            }
        }
    }

    @Test
    final void testCallCount() throws Exception {
        Config config = Config.getConfig();
        config.transactionOption = TransactionOption.LTX;
        config.transactionScope = TransactionScope.WHOLE;
        testCallCountSub(config);
        config.transactionScope = TransactionScope.CONTRACT;
        testCallCountSub(config);
        config.transactionOption = TransactionOption.OCC;
        testCallCountSub(config);
    }


    /**
     * Timerクラスのテスト、想定する使い方でExceptionが発生しないことだけテストする。
     */
    @Test
    final void testTimer() {
        Timer timer;
        TransactionId tid = new TransactionId();

        // 通常ケース
        timer = new Timer(TxOption.of());
        timer.setStartTx(tid, null);
        timer.setStartCommit(null);
        timer.setEndCommit(null, 1);
        timer.setStartTx(tid, null);
        timer.setStartCommit(null);
        timer.setEndCommit(null, 1);

        // コミット前にアボートするケース
        timer = new Timer(TxOption.of());
        timer.setStartTx(tid, null);
        timer.setAbort(null, new RuntimeException());

        // コミット後にアボートするケース
        timer = new Timer(TxOption.of());
        timer.setStartTx(tid, null);
        timer.setStartCommit(null);
        timer.setAbort(null, new RuntimeException());

        // TX開始前にAbortするケース
        timer = new Timer(TxOption.of());
        timer.setAbort(null, new RuntimeException());

        // -- 以下、使用方法が間違っているケースだが、Exceptionが出てはいけない。

        // TX開始せずコミット
        timer = new Timer(TxOption.of());
        timer.setStartCommit(null);
        timer.setEndCommit(null, 1);

        // コミット開始せずにコミット終了
        timer = new Timer(TxOption.of());
        timer.setStartTx(tid, null);
        timer.setEndCommit(null, 1);

        // TX開始せずにコミット終了
        timer = new Timer(TxOption.of());
        timer.setEndCommit(null, 1);
    }

    private void testCallCountSub(Config config) throws Exception {

        CalculationTarget T1 = createCalculationTarget("T1");
        CalculationTarget T2 = createCalculationTarget("T2");
        CalculationTarget T3 = createCalculationTarget("T3");


        CalculationTargetQueue queue;
        CalculationTask task;
        TestCalculator calculator;
        AtomicBoolean abortRequested = new AtomicBoolean(false);
        AtomicInteger tryCounter;
        AtomicInteger abortCounter = new AtomicInteger(0);


        try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {

            // Queueに処理対象の契約が含まれていない場合
            PhoneBillDbManager.initCounter();
            tryCounter = new AtomicInteger(0);
            queue = new CalculationTargetQueue(Collections.emptyList());
            task = new CalculationTask(queue, manager, config, "BID", abortRequested, tryCounter, abortCounter);
            calculator = new TestCalculator();
            task.setCalculator(calculator);
            assertNull(task.call());
            assertEquals(0, calculator.callCount);
            assertEquals(0, tryCounter.get());
            assertEquals(0, abortCounter.get());

            assertEquals(0,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.BEGIN_TX)));
            assertEquals(0,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.TRY_COMMIT)));
            assertEquals(0,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.ABORTED)));
            assertEquals(0,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.SUCCESS)));


            // Queueに処理対象の契約が1つだけ含まれているケース
            tryCounter = new AtomicInteger(0);
            queue = new CalculationTargetQueue(Collections.singletonList(T1));
            task = new CalculationTask(queue, manager, config, "BID", abortRequested, tryCounter, abortCounter);
            calculator = new TestCalculator();
            task.setCalculator(calculator);
            assertNull(task.call());
            assertEquals(1, calculator.callCount);
            assertEquals(1, tryCounter.get());
            assertEquals(0, abortCounter.get());

            assertEquals(1,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.BEGIN_TX)));
            assertEquals(1,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.TRY_COMMIT)));
            assertEquals(0,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.ABORTED)));
            assertEquals(1,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.SUCCESS)));

            // Queueに処理対象の契約が複数含まれているケース
            PhoneBillDbManager.initCounter();
            tryCounter = new AtomicInteger(0);
            queue = new CalculationTargetQueue(Arrays.asList(T1, T2, T3));
            task = new CalculationTask(queue, manager, config, "BID", abortRequested, tryCounter, abortCounter);
            calculator = new TestCalculator();
            task.setCalculator(calculator);
            assertNull(task.call());
            assertEquals(3, calculator.callCount);
            assertEquals(3, tryCounter.get());
            assertEquals(0, abortCounter.get());

            assertEquals(config.transactionScope == TransactionScope.CONTRACT ? 3 : 1,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.BEGIN_TX)));
            assertEquals(config.transactionScope == TransactionScope.CONTRACT ? 3 : 1,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.TRY_COMMIT)));
            assertEquals(0,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.ABORTED)));
            assertEquals(config.transactionScope == TransactionScope.CONTRACT ? 3 : 1,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.SUCCESS)));

            // リトライ可能ではないExceptionがスローされるケース
            PhoneBillDbManager.initCounter();
            tryCounter = new AtomicInteger(0);
            queue = new CalculationTargetQueue(Arrays.asList(T1, T2, T3));
            assertEquals(3, queue.size());
            task = new CalculationTask(queue, manager, config, "BID", abortRequested, tryCounter, abortCounter);
            calculator = new TestCalculator();
            calculator.countThrowsException = 2;
            task.setCalculator(calculator);
            assertEquals(calculator.runtimeException, task.call());
            assertEquals(2, calculator.callCount);
            assertEquals(2, tryCounter.get());
            assertEquals(1, abortCounter.get());
            assertEquals(config.transactionScope == TransactionScope.CONTRACT ? 2 : 3, queue.size());

            assertEquals(config.transactionScope == TransactionScope.CONTRACT ? 2 : 1,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.BEGIN_TX)));
            assertEquals(config.transactionScope == TransactionScope.CONTRACT ? 1 : 0,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.TRY_COMMIT)));
            assertEquals(1,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.ABORTED)));
            assertEquals(config.transactionScope == TransactionScope.CONTRACT ? 1 : 0,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.SUCCESS)));

            // Exception発生後に再度実行する
            assertNull(task.call());
            assertEquals(config.transactionScope == TransactionScope.CONTRACT ? 4 : 5, calculator.callCount);
            assertEquals(config.transactionScope == TransactionScope.CONTRACT ? 4 : 5, tryCounter.get());
            assertEquals(1, abortCounter.get());

            assertEquals(config.transactionScope == TransactionScope.CONTRACT ? 4 : 2,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.BEGIN_TX)));
            assertEquals(config.transactionScope == TransactionScope.CONTRACT ? 3 : 1,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.TRY_COMMIT)));
            assertEquals(1,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.ABORTED)));
            assertEquals(config.transactionScope == TransactionScope.CONTRACT ? 3 : 1,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.SUCCESS)));


            // リトライ可能なExceptionがスローされるケース
            PhoneBillDbManager.initCounter();
            tryCounter = new AtomicInteger(0);
            queue = new CalculationTargetQueue(Arrays.asList(T1, T2, T3));
            assertEquals(3, queue.size());
            task = new CalculationTask(queue, manager, config, "BID", abortRequested, tryCounter, abortCounter);
            calculator = new TestCalculator();
            calculator.countThrowsRetriableException = 2;
            task.setCalculator(calculator);
            assertEquals(null, task.call());
            assertEquals(config.transactionScope == TransactionScope.CONTRACT ? 4 : 5, calculator.callCount);
            assertEquals(config.transactionScope == TransactionScope.CONTRACT ? 4 : 5, tryCounter.get());
            assertEquals(0, queue.size());
            assertEquals(2, abortCounter.get());

            assertEquals(config.transactionScope == TransactionScope.CONTRACT ? 4 : 2,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.BEGIN_TX)));
            assertEquals(config.transactionScope == TransactionScope.CONTRACT ? 3 : 1,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.TRY_COMMIT)));
            assertEquals(1,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.ABORTED)));
            assertEquals(config.transactionScope == TransactionScope.CONTRACT ? 3 : 1,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.SUCCESS)));


            // 処理開始前にAbortが要求されたケース
            PhoneBillDbManager.initCounter();
            tryCounter = new AtomicInteger(0);
            queue = new CalculationTargetQueue(Arrays.asList(T1, T2, T3));
            assertEquals(3, queue.size());
            task = new CalculationTask(queue, manager, config, "BID", abortRequested, tryCounter, abortCounter);
            calculator = new TestCalculator();
            calculator.countThrowsRetriableException = 2;
            task.setCalculator(calculator);
            abortRequested.set(true);
            assertEquals(null, task.call());
            assertEquals(0, calculator.callCount);
            abortRequested.set(false);
            assertEquals(2, abortCounter.get());

            assertEquals(0,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.BEGIN_TX)));
            assertEquals(0,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.TRY_COMMIT)));
            assertEquals(0,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.ABORTED)));
            assertEquals(0,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.SUCCESS)));


            // 処理中にAbortが要求されたケース

            ExecutorService service1 = Executors.newSingleThreadExecutor();

            PhoneBillDbManager.initCounter();
            tryCounter = new AtomicInteger(0);
            queue = new CalculationTargetQueue(Arrays.asList(T1, T2, T3));
            task = new CalculationTask(queue, manager, config, "BID", abortRequested, tryCounter, abortCounter);
            calculator = new TestCalculator();
            calculator.countWait = 2; // 2番目の処理対象の処理中に待たされる
            task.setCalculator(calculator);

            final var finalTask = task;
            Runnable runnable = new Runnable() {

                @Override
                public void run() {
                    try {
                        assertEquals(null, finalTask.call());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            };
            Future<?> f1 = service1.submit(runnable);
            calculator.beginWait.await(); // 処理が途中まで進むのを待つ
            assertEquals(2, calculator.callCount);
            assertEquals(2, tryCounter.get());

            assertEquals(config.transactionScope == TransactionScope.CONTRACT ? 2 : 1,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.BEGIN_TX)));
            assertEquals(config.transactionScope == TransactionScope.CONTRACT ? 1 : 0,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.TRY_COMMIT)));
            assertEquals(0,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.ABORTED)));
            assertEquals(config.transactionScope == TransactionScope.CONTRACT ? 1 : 0,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.SUCCESS)));

            abortRequested.set(true);
            calculator.endWait.countDown(); // 処理を継続する
            f1.get();
            service1.shutdown();
            abortRequested.set(false);

            assertEquals(2, calculator.callCount);
            assertEquals(2, tryCounter.get());

            assertEquals(config.transactionScope == TransactionScope.CONTRACT ? 2 : 1,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.BEGIN_TX)));
            assertEquals(config.transactionScope == TransactionScope.CONTRACT ? 2 : 1,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.TRY_COMMIT)));
            assertEquals(0,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.ABORTED)));
            assertEquals(config.transactionScope == TransactionScope.CONTRACT ? 2 : 1,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.SUCCESS)));
            calculator = null;



            // 他のタスクが処理中のTargetの処理が終了するまで待たされるケース
            // scopeがWHOLEの場合は、他のタスクが処理中の契約を除いてcommitを実行、
            // その後他のタスクがrevertした契約を処理するので、2回TXが実行される。

            ExecutorService service2 = Executors.newSingleThreadExecutor();
            PhoneBillDbManager.initCounter();
            tryCounter = new AtomicInteger(0);
            queue = new CalculationTargetQueue(Arrays.asList(T1, T2, T3));
            task = new CalculationTask(queue, manager, config, "BID", abortRequested, tryCounter, abortCounter);
            calculator = new TestCalculator();
            task.setCalculator(calculator);

            long sleepTime = 300;
            AnotherTask anotherTask = new AnotherTask(queue, sleepTime);
            Future<?> f2 = service2.submit(anotherTask);
            long start = System.currentTimeMillis();
            assertNull(task.call());
            assertEquals(3, calculator.callCount);
            assertEquals(3, tryCounter.get());

            assertEquals(config.transactionScope == TransactionScope.CONTRACT ? 3 : 2,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.BEGIN_TX)));
            assertEquals(config.transactionScope == TransactionScope.CONTRACT ? 3 : 2,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.TRY_COMMIT)));
            assertEquals(0,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.ABORTED)));
            assertEquals(config.transactionScope == TransactionScope.CONTRACT ? 3 : 2,
                    PhoneBillDbManager.getCounter(task.getTxOption().getCounterKey(CounterName.SUCCESS)));


            long elapsed = System.currentTimeMillis() - start;
            assertTrue(elapsed > sleepTime, "elapsed time = " + elapsed + ", sleep time =" + sleepTime);
            f2.get();
            service2.shutdown();

        }
    }

    private static class AnotherTask implements Runnable {
        CalculationTarget target;
        CalculationTargetQueue queue;
        long sleepTime;

        public AnotherTask(CalculationTargetQueue queue, long sleepTime) {
            this.queue = queue;
            this.sleepTime = sleepTime;
            target = queue.poll();
        }

        @Override
        public void run() {
            try {
                Thread.sleep(sleepTime);
                queue.revert(target);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }




    private CalculationTarget  createCalculationTarget(String label) {
        Contract c = Contract.create(label, "2000-01-01", null, null);
        return new CalculationTarget(c, null, null, null, null, false);
    }


    private static class  TestCalculator implements Calculator {
        int callCount = 0;
        int countThrowsException = -1;
        int countThrowsRetriableException = -1;
        int countWait = -1;
        CountDownLatch beginWait = new CountDownLatch(1);
        CountDownLatch endWait = new CountDownLatch(1);

        RuntimeException runtimeException = new RuntimeException();
        RetryOverRuntimeException retriableException = new RetryOverRuntimeException();

        @Override
        public int doCalc(CalculationTarget target) {
            callCount++;
            if (callCount == countThrowsException) {
                throw runtimeException;
            }
            if (callCount == countThrowsRetriableException) {
                throw retriableException;
            }
            if (callCount == countWait) {
                beginWait.countDown();
                try {
                    endWait.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            return 1;
        }
    }
}
