package com.example.nedo.app.billing;

import java.sql.Date;

import com.example.nedo.db.Contract;

/**
 * 料金計算対象の契約と、計算ロジックを格納するクラス
 *
 */
public class CalculationTarget {
	/**
	 * 計算対象の契約
	 */
	private Contract contract;
	/**
	 * 請求金額の計算に使用するクラス
	 */
	private BillingCalculator billingCalculator;


	/**
	 * １通話の料金計算に使用するクラス
	 */
	private CallChargeCalculator callChargeCalculator;

	/**
	 * 計算対象月の最初の日
	 */
	private Date start;
	/**
	 * 計算対象月の翌月の最初の日
	 */
	private Date end;

	/**
	 * 計算対象がないことを示すフラグ
	 */
	private boolean endOfTask = false;


	/**
	 * @param contract
	 * @param billingCalculator
	 * @param callChargeCalculator
	 * @param start
	 * @param end
	 * @param endOfTask
	 */
	public CalculationTarget(Contract contract, BillingCalculator billingCalculator,
			CallChargeCalculator callChargeCalculator, Date start, Date end, boolean endOfTask) {
		this.contract = contract;
		this.billingCalculator = billingCalculator;
		this.callChargeCalculator = callChargeCalculator;
		this.start = start;
		this.end = end;
		this.endOfTask = endOfTask;
	}

	public static CalculationTarget getEndOfTask() {
		CalculationTarget target = new CalculationTarget(null, null, null, null, null, true);
		return target;
	}


	/**
	 * @return contract
	 */
	public Contract getContract() {
		return contract;
	}


	/**
	 * @return billingCalculator
	 */
	public BillingCalculator getBillingCalculator() {
		return billingCalculator;
	}


	/**
	 * @return callChargeCalculator
	 */
	public CallChargeCalculator getCallChargeCalculator() {
		return callChargeCalculator;
	}


	/**
	 * @return endOfTask
	 */
	public boolean isEndOfTask() {
		return endOfTask;
	}


	/**
	 * @return start
	 */
	public Date getStart() {
		return start;
	}


	/**
	 * @return end
	 */
	public Date getEnd() {
		return end;
	}
}
