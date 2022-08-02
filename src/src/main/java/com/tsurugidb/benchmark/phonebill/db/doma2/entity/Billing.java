package com.tsurugidb.benchmark.phonebill.db.doma2.entity;

import java.sql.Date;

import org.seasar.doma.Column;
import org.seasar.doma.Entity;
import org.seasar.doma.Table;

@Entity
@Table(name = "billing")
public class Billing {
	/**
	* 電話番号
	*/
	@Column(name ="phone_number")
	public String phoneNumber;

	/**
	 * 対象年月
	 */
	@Column(name ="target_month")
	public Date targetMonth;

	/**
	 * 基本料金
	 */
	@Column(name ="basic_charge")
	public int basicCharge;

	/**
	 * 従量料金
	 */
	@Column(name ="metered_charge")
	public int meteredCharge;

	/**
	* 請求金額
	*/
	@Column(name ="billing_amount")
	public int billingAmount;

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Billing [phoneNumber=");
		builder.append(phoneNumber);
		builder.append(", targetMonth=");
		builder.append(targetMonth);
		builder.append(", basicCharge=");
		builder.append(basicCharge);
		builder.append(", meteredCharge=");
		builder.append(meteredCharge);
		builder.append(", billingAmount=");
		builder.append(billingAmount);
		builder.append("]");
		return builder.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + basicCharge;
		result = prime * result + billingAmount;
		result = prime * result + meteredCharge;
		result = prime * result + ((phoneNumber == null) ? 0 : phoneNumber.hashCode());
		result = prime * result + ((targetMonth == null) ? 0 : targetMonth.hashCode());
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
		Billing other = (Billing) obj;
		if (basicCharge != other.basicCharge)
			return false;
		if (billingAmount != other.billingAmount)
			return false;
		if (meteredCharge != other.meteredCharge)
			return false;
		if (phoneNumber == null) {
			if (other.phoneNumber != null)
				return false;
		} else if (!phoneNumber.equals(other.phoneNumber))
			return false;
		if (targetMonth == null) {
			if (other.targetMonth != null)
				return false;
		} else if (!targetMonth.equals(other.targetMonth))
			return false;
		return true;
	}
}
