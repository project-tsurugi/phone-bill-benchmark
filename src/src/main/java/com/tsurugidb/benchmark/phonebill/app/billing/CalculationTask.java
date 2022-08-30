package com.tsurugidb.benchmark.phonebill.app.billing;

import java.sql.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.Config.TransactionScope;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
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
			AtomicBoolean abortRequested) {
		this.queue = queue;
		this.config = config;
		this.batchExecId = batchExecId;
		this.abortRequested = abortRequested;
		this.manager = manager;
		billingDao = manager.getBillingDao();
		historyDao = manager.getHistoryDao();
	}

	@Override
	public Exception call() throws Exception {
		try {
			LOG.debug("Calculation task started.");
			for (;;) {
				CalculationTarget target;
				try {
					target = queue.take();
					LOG.debug(queue.getStatus());
				} catch (InterruptedException e) {
					LOG.debug("InterruptedException caught and continue taking calculation_target", e);
					continue;
				}
				if (target.isEndOfTask() || abortRequested.get() == true) {
					LOG.debug("Calculation task finished normally.");
					return null;
				}
				// TODO: リトライ回数を指定可能にする
				for (;;) {
					try {
						doCalc(target);
						break;
					} catch (RuntimeException e) {
						if (manager.isRetriable(e) && config.transactionScope == TransactionScope.CONTRACT) {
							manager.rollback();
						} else {
							throw e;
						}
					}
				}
			}
		} catch (Exception e) {
			return e;
		}
	}

	/**
	 * 料金計算のメインロジック
	 *
	 * @param target
	 */
	private void doCalc(CalculationTarget target) {
		Contract contract = target.getContract();
		LOG.debug("Start calcuration for  contract: {}.", contract);
		List<History> histories = historyDao.getHistories(target);

		CallChargeCalculator callChargeCalculator = target.getCallChargeCalculator();
		BillingCalculator billingCalculator = target.getBillingCalculator();
		for(History h: histories) {
			if (h.getTimeSecs() < 0) {
				throw new RuntimeException("Negative time: " + h.getTimeSecs());
			}
			h.setCharge(callChargeCalculator.calc(h.getTimeSecs()));
			billingCalculator.addCallCharge(h.getCharge());
		}
		historyDao.batchUpdate(histories);
		updateBilling(contract, billingCalculator, target.getStart());
		if (config.transactionScope == TransactionScope.CONTRACT) {
			manager.commit();
		}
		LOG.debug("End calcuration for  contract: {}.", contract);
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
}
