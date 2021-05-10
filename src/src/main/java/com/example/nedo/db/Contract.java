package com.example.nedo.db;

import java.sql.Date;

public class Contract implements Cloneable {
	/**
	 * 電話番号
	 */
	public String phoneNumber;

	/**
	 * 契約開始日
	 */
	public Date startDate;

	/**
	 * 契約終了日
	 */
	public Date endDate;

	/**
	 * 料金計算ルール
	 */
	public String rule;

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Contract [phone_number=");
		builder.append(phoneNumber);
		builder.append(", start_date=");
		builder.append(startDate);
		builder.append(", end_date=");
		builder.append(endDate);
		builder.append(", rule=");
		builder.append(rule);
		builder.append("]");
		return builder.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((endDate == null) ? 0 : endDate.hashCode());
		result = prime * result + ((phoneNumber == null) ? 0 : phoneNumber.hashCode());
		result = prime * result + ((rule == null) ? 0 : rule.hashCode());
		result = prime * result + ((startDate == null) ? 0 : startDate.hashCode());
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
		Contract other = (Contract) obj;
		if (endDate == null) {
			if (other.endDate != null)
				return false;
		} else if (!endDate.equals(other.endDate))
			return false;
		if (phoneNumber == null) {
			if (other.phoneNumber != null)
				return false;
		} else if (!phoneNumber.equals(other.phoneNumber))
			return false;
		if (rule == null) {
			if (other.rule != null)
				return false;
		} else if (!rule.equals(other.rule))
			return false;
		if (startDate == null) {
			if (other.startDate != null)
				return false;
		} else if (!startDate.equals(other.startDate))
			return false;
		return true;
	}

	@Override
	public Contract clone()  {
		try {
			return (Contract) super.clone();
		} catch (CloneNotSupportedException e) {
			throw new InternalError(e);
		}
	}
}
