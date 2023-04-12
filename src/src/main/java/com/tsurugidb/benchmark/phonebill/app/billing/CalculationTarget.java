package com.tsurugidb.benchmark.phonebill.app.billing;

import java.sql.Date;

import com.tsurugidb.benchmark.phonebill.db.entity.Contract;

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
	private Long start;
	/**
	 * 計算対象月の翌月の最初の日
	 */
	private Long end;

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
		this.start = start == null ? null : start.getTime();
		this.end = end == null ? null : end.getTime();
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
	 * @return start
	 */
	public Date getStart() {
		return start == null ? null : new Date(start);
	}

	/**
	 * @return end
	 */
	public Date getEnd() {
		return end == null ? null : new Date(end);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((billingCalculator == null) ? 0 : billingCalculator.hashCode());
		result = prime * result + ((callChargeCalculator == null) ? 0 : callChargeCalculator.hashCode());
		result = prime * result + ((contract == null) ? 0 : contract.hashCode());
		result = prime * result + ((end == null) ? 0 : end.hashCode());
		result = prime * result + ((start == null) ? 0 : start.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CalculationTarget other = (CalculationTarget) obj;
		if (billingCalculator == null) {
			if (other.billingCalculator != null)
				return false;
		} else if (!billingCalculator.equals(other.billingCalculator))
			return false;
		if (callChargeCalculator == null) {
			if (other.callChargeCalculator != null)
				return false;
		} else if (!callChargeCalculator.equals(other.callChargeCalculator))
			return false;
		if (contract == null) {
			if (other.contract != null)
				return false;
		} else if (!contract.equals(other.contract))
			return false;
		if (end == null) {
			if (other.end != null)
				return false;
		} else if (!end.equals(other.end))
			return false;
		if (start == null) {
			if (other.start != null)
				return false;
		} else if (!start.equals(other.start))
			return false;
		return true;
	}
}
