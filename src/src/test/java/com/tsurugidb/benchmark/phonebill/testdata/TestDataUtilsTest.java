package com.tsurugidb.benchmark.phonebill.testdata;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Random;

import org.junit.jupiter.api.Test;

class TestDataUtilsTest {

	@Test
	void testGetRandomLong() {
		long min = Long.MAX_VALUE;
		long max = Long.MIN_VALUE;
		for (int i = 0; i < 10000; i++) {
			long val = TestDataUtils.getRandomLong(new Random(), 100, 120);
			min = Math.min(min, val);
			max = Math.max(max, val);
		}
		assertEquals(100, min);
		assertEquals(119, max);
	}

	@Test
	void testGetRandomInt() {
		int min = Integer.MAX_VALUE;
		int max = Integer.MIN_VALUE;
		for (int i = 0; i < 10000; i++) {
			int val = TestDataUtils.getRandomInt(new Random(), 55, 98);
			min = Math.min(min, val);
			max = Math.max(max, val);
		}
		assertEquals(55, min);
		assertEquals(97, max);
	}

}
