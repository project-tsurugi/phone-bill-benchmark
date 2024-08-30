/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

