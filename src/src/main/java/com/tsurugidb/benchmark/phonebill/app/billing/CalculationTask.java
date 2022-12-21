package com.tsurugidb.benchmark.phonebill.app.billing;

import java.sql.Date;
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
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.TxOption.Table;
import com.tsurugidb.benchmark.phonebill.db.dao.BillingDao;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;
import com.tsurugidb.benchmark.phonebill.db.entity.Billing;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.History;

public class CalculationTask implements Callable<Exception> {
    private static final Logger LOG = LoggerFactory.getLogger(CalculationTask.class);
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
			txOption = TxOption.ofOCC(config.transactionScope == TransactionScope.CONTRACT ? Integer.MAX_VALUE : 0,
					"CalculationTask");
			break;
		case LTX:
			txOption = TxOption.ofLTX(config.transactionScope == TransactionScope.CONTRACT ? Integer.MAX_VALUE : 0,
					"CalculationTask", Table.HISTORY, Table.BILLING);
			break;
		}
	}

	@Override
	public Exception call() throws Exception {
		// TODO スレッド終了時にトランザクションが終了してしまうが、すべてのスレッドの処理終了を待って
		// commit or rollbackするようにしたい。
		LOG.info("Calculation task started.");


		if (config.transactionScope == TransactionScope.CONTRACT) {
			while (continueLoop()) {
				CalculationTarget target = queue.take();
				if (target == null) {
					continue;
				}
				LOG.debug(queue.getStatus());
				TransactionId tid = new TransactionId();
				try {
					AtomicInteger tryInThisTx = new AtomicInteger(0);
					LOG.debug("Start calculation for  contract: {}.", target.getContract());
					manager.execute(txOption, () -> {
						tryInThisTx.incrementAndGet();
						tryCounter.incrementAndGet();
						tid.set(manager.getTransactionId());
						LOG.debug("Transaction started, tid = {}, txOption = {}, key = {}, tryCount = {}", tid, txOption,
								target.getContract().getPhoneNumber(), tryInThisTx);
						calculator.doCalc(target);
					});
					abortCounter.addAndGet(tryInThisTx.get() - 1);
					queue.success(target);
					LOG.debug("Transaction completed, tid = {}, contract = {}.", tid, target.getContract());
					nCalculated++;
				} catch (RuntimeException e) {
					abortCounter.incrementAndGet();
					queue.revert(target);
					LOG.error("Transaction aborted, tid = {}, contract = {}.", tid, target.getContract(), e);
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
					manager.execute(txOption, () -> {
						tid.set(manager.getTransactionId());
						LOG.debug("Transaction started, tid = {}, txOption = {}, tryCount = {}", tid, txOption,
								tryCounter, manager.getTransactionId());
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
							calculator.doCalc(target);
						}
					});
					nCalculated += list.size();
					LOG.debug("Transaction completed, tid = {}, contract = {}.", tid, getPhoneNumbers(list));
					queue.success(list);
				} catch (RuntimeException e) {
					abortCounter.incrementAndGet();
					// 処理対象をキューに戻す
					queue.revert(list);
					LOG.debug("Transaction aborted, tid = {}, contract = {}.", tid, getPhoneNumbers(list));
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
		 */
		void doCalc(CalculationTarget target);
	}

	protected class CalculatorImpl implements Calculator {
		@Override
		public void doCalc(CalculationTarget target) {
			LOG.debug("Start calculation for  contract: {}.", target.getContract());

			Contract contract = target.getContract();
			List<History> histories = historyDao.getHistories(target);

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
		}
	}

	private static class TransactionId {
		private String tid ="none";

		void set(String tid) {
			this.tid = tid;
		}

		@Override
		public String toString() {
			return tid;
		}
	}
}
