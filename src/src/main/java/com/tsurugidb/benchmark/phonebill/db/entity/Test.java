package com.tsurugidb.benchmark.phonebill.db.entity;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class Test {

	public static void main(String[] args) {
		Instant i = Instant.now();
		long l = System.currentTimeMillis();
		LocalDateTime localDateTime = LocalDateTime.ofInstant(i, ZoneId.systemDefault());
		System.out.println(localDateTime);
		System.out.println(new Timestamp(i.toEpochMilli()));
		System.out.println(i.toEpochMilli() - l);
		Timestamp ts = Timestamp.valueOf(localDateTime);
		System.out.println(ts);
		System.out.println(i.toEpochMilli() - ts.getTime());
	}

}
