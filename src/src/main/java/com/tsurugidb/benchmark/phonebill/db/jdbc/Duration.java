package com.tsurugidb.benchmark.phonebill.db.jdbc;

import java.sql.Date;

/**
 * 期間を表すクラス
 *
 */
public class Duration {
	public Long start;
	public Long end;

	public Duration(Date start, Date end) {
		this.start = start.getTime();
		this.end = end == null ? null : end.getTime();
	}

	public Duration(Long start, Long end) {
		this.start = start;
		this.end = end;
	}

	public Date getStatDate() {
		return new Date(start);
	}

	public Date getEndDate() {
		return end == null ? null : new Date(end);
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
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
		Duration other = (Duration) obj;
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

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Duration [start=");
		builder.append(new Date(start));
		builder.append(", end=");
		builder.append(new Date(end));
		builder.append("]");
		return builder.toString();
	}
}

