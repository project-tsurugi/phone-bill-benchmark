package com.tsurugidb.benchmark.phonebill.db.entity;

import java.sql.Date;

import com.tsurugidb.benchmark.phonebill.util.DateUtils;

public class Contract implements Cloneable {
	/**
	 * 電話番号
	 */
	private String phoneNumber;

	/**
	 * 契約開始日
	 */
	private Date startDate;

	/**
	 * 契約終了日
	 */
	private Date endDate;

	/**
	 * 料金計算ルール
	 */
	private String rule;

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

	public Key getKey() {
		return createKey(phoneNumber, startDate);
	}


	/**
	 * Contractsの主キー
	 */
	public static class Key {
		private String phoneNumber;
		private Date startDate;

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((phoneNumber == null) ? 0 : phoneNumber.hashCode());
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
			Key other = (Key) obj;
			if (phoneNumber == null) {
				if (other.phoneNumber != null)
					return false;
			} else if (!phoneNumber.equals(other.phoneNumber))
				return false;
			if (startDate == null) {
				if (other.startDate != null)
					return false;
			} else if (!startDate.equals(other.startDate))
				return false;
			return true;
		}

		public String getPhoneNumber() {
			return phoneNumber;
		}

		public void setPhoneNumber(String phoneNumber) {
			this.phoneNumber = phoneNumber;
		}

		public Date getStartDate() {
			return startDate;
		}

		public void setStartDate(Date startDate) {
			this.startDate = startDate;
		}
	}

	/**
	 * 電話番号と契約開始日を指定してKeyを生成する
	 *
	 * @param phoneNumber
	 * @param startDate
	 * @return
	 */
	public static Key createKey(String phoneNumber, Date startDate) {
		Key key = new Key();
		key.setPhoneNumber(phoneNumber);
		key.setStartDate(startDate);
		return key;
	}

	public String getPhoneNumber() {
		return phoneNumber;
	}

	public void setPhoneNumber(String phoneNumber) {
		this.phoneNumber = phoneNumber;
	}

	public Date getStartDate() {
		return startDate;
	}

	public long getStartDateAsLong() {
		return startDate.getTime();
	}

	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}

	public void setStartDate(long startDate) {
		this.startDate = new Date(startDate);
	}

	public Date getEndDate() {
		return endDate;
	}

	// Iceaxe用のメソッド TsurguiがTime型を未サポートなので代わりにlongを使う。
	// またTsurugiがis not nullを未サポートなので代わりにLong.MAX_VALUEを使う。
	public Long getEndDateAsLong() {
		return endDate == null ? Long.MAX_VALUE : endDate.getTime();
	}

	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}

	// Iceaxe用のメソッド TsurguiがTime型を未サポートなので代わりにlongを使う。
	// またTsurugiがis not nullを未サポートなので代わりにLong.MAX_VALUEを使う。
	public void setEndDate(Long endDate) {
		this.endDate = endDate == Long.MAX_VALUE  ? null : new Date(endDate);
	}


	public String getRule() {
		return rule;
	}

	public void setRule(String rule) {
		this.rule = rule;
	}

	public static Contract create(String phoneNumber, String startDate, String endDate, String rule) {
		Contract c = new Contract();
		c.phoneNumber = phoneNumber;
		c.startDate = DateUtils.toDate(startDate);
		c.endDate =  endDate == null ? null :  DateUtils.toDate(endDate);
		c.rule = rule;
		return c;
	}
}
