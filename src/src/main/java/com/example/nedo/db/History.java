package com.example.nedo.db;

import java.sql.Timestamp;

public class History implements Cloneable{
	/**
	 * 発信者電話番号
	 */
	public String callerPhoneNumber;

	/**
	 * 受信者電話番号
	 */
	public String recipientPhoneNumber;

	/**
	 * 料金区分(発信者負担(C)、受信社負担(R))
	 */

	public String paymentCategorty;
	 /**
	 * 通話開始時刻
	 */

	public Timestamp startTime;
	 /**
	 * 通話時間(秒)
	 */

	public int timeSecs;

	/**
	 * 料金
	 */
	public Integer charge;

	/**
	 * 削除フラグ
	 */
	public boolean df = false;

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("History [callerPhoneNumber=");
		builder.append(callerPhoneNumber);
		builder.append(", recipientPhoneNumber=");
		builder.append(recipientPhoneNumber);
		builder.append(", paymentCategorty=");
		builder.append(paymentCategorty);
		builder.append(", startTime=");
		builder.append(startTime);
		builder.append(", timeSecs=");
		builder.append(timeSecs);
		builder.append(", charge=");
		builder.append(charge);
		builder.append(", df=");
		builder.append(df);
		builder.append("]");
		return builder.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((callerPhoneNumber == null) ? 0 : callerPhoneNumber.hashCode());
		result = prime * result + ((charge == null) ? 0 : charge.hashCode());
		result = prime * result + (df ? 1231 : 1237);
		result = prime * result + ((paymentCategorty == null) ? 0 : paymentCategorty.hashCode());
		result = prime * result + ((recipientPhoneNumber == null) ? 0 : recipientPhoneNumber.hashCode());
		result = prime * result + ((startTime == null) ? 0 : startTime.hashCode());
		result = prime * result + timeSecs;
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
		History other = (History) obj;
		if (callerPhoneNumber == null) {
			if (other.callerPhoneNumber != null)
				return false;
		} else if (!callerPhoneNumber.equals(other.callerPhoneNumber))
			return false;
		if (charge == null) {
			if (other.charge != null)
				return false;
		} else if (!charge.equals(other.charge))
			return false;
		if (df != other.df)
			return false;
		if (paymentCategorty == null) {
			if (other.paymentCategorty != null)
				return false;
		} else if (!paymentCategorty.equals(other.paymentCategorty))
			return false;
		if (recipientPhoneNumber == null) {
			if (other.recipientPhoneNumber != null)
				return false;
		} else if (!recipientPhoneNumber.equals(other.recipientPhoneNumber))
			return false;
		if (startTime == null) {
			if (other.startTime != null)
				return false;
		} else if (!startTime.equals(other.startTime))
			return false;
		if (timeSecs != other.timeSecs)
			return false;
		return true;
	}

	@Override
	public History clone()  {
		try {
			return (History) super.clone();
		} catch (CloneNotSupportedException e) {
			throw new InternalError(e);
		}
	}

}
