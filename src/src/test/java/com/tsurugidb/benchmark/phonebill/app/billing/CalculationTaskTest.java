package com.tsurugidb.benchmark.phonebill.app.billing;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.Config.TransactionScope;
import com.tsurugidb.benchmark.phonebill.app.billing.CalculationTask.Calculator;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.RetryOverRuntimeException;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;

class CalculationTaskTest {

	@BeforeAll
	static void setUpBeforeClass() throws Exception {
	}

	@AfterAll
	static void tearDownAfterClass() throws Exception {
	}

	@BeforeEach
	void setUp() throws Exception {
	}

	@AfterEach
	void tearDown() throws Exception {
	}


	@Test
	final void testCall() throws Exception {
		Config config = Config.getConfig();
		config.transactionScope = TransactionScope.WHOLE;
		testCallSub(config);
		config.transactionScope = TransactionScope.CONTRACT;
		testCallSub(config);
	}


	private void testCallSub(Config config) throws Exception {
		CalculationTarget T1 = createCalculationTarget("T1");
		CalculationTarget T2 = createCalculationTarget("T2");
		CalculationTarget T3 = createCalculationTarget("T3");


		PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config);
		CalculationTargetQueue queue;
		CalculationTask task;
		TestCalculator calculator;
		AtomicBoolean abortRequested = new AtomicBoolean(false);;

		// Queueが処理対象の契約が含まれていない場合
		queue= new CalculationTargetQueue(1);
		queue.setEndOfTask();
		task = new CalculationTask(queue, manager, config, "BID", abortRequested);
		calculator = new TestCalculator();
		task.setCalculator(calculator);
		assertNull(task.call());
		assertEquals(0, calculator.callCount);

		// Queueに処理対象の契約が1つだけ含まれているケース

		queue= new CalculationTargetQueue(1);
		queue.put(T1);
		queue.setEndOfTask();
		task = new CalculationTask(queue, manager, config, "BID", abortRequested);
		calculator = new TestCalculator();
		task.setCalculator(calculator);
		assertNull(task.call());
		assertEquals(1, calculator.callCount);


		// Queueに処理対象の契約が複数含まれているケース
		queue= new CalculationTargetQueue(1);
		queue.put(T1);
		queue.put(T2);
		queue.put(T3);
		queue.setEndOfTask();
		task = new CalculationTask(queue, manager, config, "BID", abortRequested);
		calculator = new TestCalculator();
		task.setCalculator(calculator);
		assertNull(task.call());
		assertEquals(3, calculator.callCount);


		// リトライ可能ではないExceptionがスローされるケース
		queue= new CalculationTargetQueue(1);
		queue.put(T1);
		queue.put(T2);
		queue.put(T3);
		queue.setEndOfTask();
		assertEquals(4, queue.size());
		task = new CalculationTask(queue, manager, config, "BID", abortRequested);
		calculator = new TestCalculator();
		calculator.countThrowsException = 2;
		task.setCalculator(calculator);
		assertEquals(calculator.runtimeException,  task.call());
		assertEquals(2, calculator.callCount);

		assertEquals(config.transactionScope == TransactionScope.CONTRACT ? 3: 4, queue.size());

		// Exception発生後に再度実行する
		assertNull(task.call());
		assertEquals(config.transactionScope == TransactionScope.CONTRACT ? 4: 5, calculator.callCount);


		// リトライ可能なExceptionがスローされるケース
		queue= new CalculationTargetQueue(1);
		queue.put(T1);
		queue.put(T2);
		queue.put(T3);
		queue.setEndOfTask();
		assertEquals(4, queue.size());
		task = new CalculationTask(queue, manager, config, "BID", abortRequested);
		calculator = new TestCalculator();
		calculator.countThrowsRetriableException = 2;
		task.setCalculator(calculator);
		assertEquals(config.transactionScope == TransactionScope.CONTRACT ? calculator.retriableException : null,
				task.call());
		assertEquals(config.transactionScope == TransactionScope.CONTRACT ? 2: 5, calculator.callCount);
		assertEquals(config.transactionScope == TransactionScope.CONTRACT ? 3: 0, queue.size());

		// Abortが要求されたケース
		queue= new CalculationTargetQueue(1);
		queue.put(T1);
		queue.put(T2);
		queue.put(T3);
		queue.setEndOfTask();
		assertEquals(4, queue.size());
		task = new CalculationTask(queue, manager, config, "BID", abortRequested);
		calculator = new TestCalculator();
		calculator.countThrowsRetriableException = 2;
		task.setCalculator(calculator);
		abortRequested.set(true);
		assertEquals(null, task.call());
		assertEquals(0, calculator.callCount);
	}




	private CalculationTarget  createCalculationTarget(String label) {
		Contract c = Contract.create(label, "2000-01-01", null, null);
		return new CalculationTarget(c, null, null, null, null, false);
	}


	private class  TestCalculator implements Calculator {
		int callCount = 0;
		int countThrowsException = -1;
		int countThrowsRetriableException = -1;

		RuntimeException runtimeException = new RuntimeException();
		RetryOverRuntimeException retriableException = new RetryOverRuntimeException();

		@Override
		public void doCalc(CalculationTarget target) {
			callCount++;
			if (callCount == countThrowsException) {
				throw runtimeException;
			}
			if (callCount == countThrowsRetriableException) {
				throw retriableException;
			}
		}
	}
}
