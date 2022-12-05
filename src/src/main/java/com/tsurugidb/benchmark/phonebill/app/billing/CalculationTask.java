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
import com.tsurugidb.benchmark.phonebill.db.dao.BillingDao;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;
import com.tsurugidb.benchmark.phonebill.db.entity.Billing;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.sql.proto.SqlRequest.TransactionPriority;

public class CalculationTask implements Callable<Exception> {
    private static final Logger LOG = LoggerFactory.getLogger(CalculationTask.class);
    private Config config;
    private String batchExecId;
    private AtomicBoolean abortRequested;
    private AtomicInteger tryCounter;
    private AtomicInteger abortCounter;
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
	}

	@Override
	public Exception call() throws Exception {
		// TODO スレッド終了時にトランザクションが終了してしまうが、すべてのスレッドの処理終了を待って
		// commit or rollbackするようにしたい。
		LOG.info("Calculation task started.");

		TgTxOption txOption = null;
		switch (config.transactionOption) {
		case OCC:
			txOption = TgTxOption.ofOCC();
			break;
		case LTX:
			txOption = TgTxOption.ofLTX("history", "billing").priority(TransactionPriority.INTERRUPT_EXCLUDE);
			break;
		}

		if (config.transactionScope == TransactionScope.CONTRACT) {
			int n = 0;
			for (;;) {
				CalculationTarget target = queue.take();
				LOG.debug(queue.getStatus());
				if (target == null || abortRequested.get() == true) {
					LOG.info("Calculation task finished normally, number of calculated contracts = {}).", n);
					return null;
				}
				try {
					AtomicInteger tryInThisTx = new AtomicInteger(0);
					LOG.debug("Start calculation for  contract: {}.", target.getContract());
					String str = txOption.toString();
					manager.execute(TxOption.of(Integer.MAX_VALUE, TgTmSetting.ofAlways(txOption)), () -> {
						tryInThisTx.incrementAndGet();
						tryCounter.incrementAndGet();
						LOG.debug("start tansaction with txOption = {}, key = {}, tryCount = {}", str,
								target.getContract().getPhoneNumber(), tryInThisTx);
						calculator.doCalc(target);
					});
					abortCounter.addAndGet(tryInThisTx.get() - 1);
					queue.success(target);
					LOG.debug("End calculation for contract: {}.", target.getContract());
					n++;
				} catch (RuntimeException e) {
					abortCounter.incrementAndGet();
					queue.revert(target);
					LOG.error("Abort calculation for contract: " + target.getContract() + ".", e);
					LOG.debug("Calculation target returned to queue and the task will be aborted.");
					return e;
				}
			}
		} else {
			for (;;) {
				List<CalculationTarget> list = new ArrayList<>();
				String str = txOption.toString();
				try {
					manager.execute(TxOption.of(0, TgTmSetting.of(txOption)), () -> {
						LOG.debug("start tansaction with txOption = {}, tryCount = {}", str, tryCounter);
						for (;;) {
							CalculationTarget target;
							target = queue.poll();
							if (target != null) {
								list.add(target);
							}
							LOG.debug(queue.getStatus());
							if (target == null || abortRequested.get() == true) {
								break;
							}
							tryCounter.incrementAndGet();
							LOG.debug("Start calculation for  contract: {}.", target.getContract());
							calculator.doCalc(target);
						}
					});
					queue.success(list);
					if (abortRequested.get() == false && !queue.finished()) {
						Thread.sleep(10);
						continue;
					}
					LOG.info("Calculation task finished normally, contracts = {}).", getContracts(list));
					return null;
				} catch (RuntimeException e) {
					abortCounter.incrementAndGet();
					// 処理対象をキューに戻す
					queue.revert(list);
					LOG.info("Calculation task aborted, contracts = {}).", getContracts(list));
					if (e instanceof RetryOverRuntimeException) {
						LOG.debug(
								"Transaction aborted with retriable exception and calculation targets returned to queue.",
								e);
						continue;
					} else {
						LOG.debug("Calculation targets returned to queue and the task will be aborted.", e);
						return e;
					}
				}
			}
		}
	}

	static String getContracts(List<CalculationTarget> list) {
		return String.join(",", list.stream().map(t -> t.getContract().toString()).collect(Collectors.toList()));
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

}
