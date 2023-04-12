package com.tsurugidb.benchmark.phonebill.testdata;

/**
 * historyのキー値、高速化のためにデータ型にStringとjava.sqlDateではなくlongを用いる。
 *
 */
public class HistoryKey {
	public long startTime;
	public long callerPhoneNumber;

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (callerPhoneNumber ^ (callerPhoneNumber >>> 32));
		result = prime * result + (int) (startTime ^ (startTime >>> 32));
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
		HistoryKey other = (HistoryKey) obj;
		if (callerPhoneNumber != other.callerPhoneNumber)
			return false;
		if (startTime != other.startTime)
			return false;
		return true;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Key [startTime=");
		builder.append(startTime);
		builder.append(", callerPhoneNumber=");
		builder.append(callerPhoneNumber);
		builder.append("]");
		return builder.toString();
	}
}