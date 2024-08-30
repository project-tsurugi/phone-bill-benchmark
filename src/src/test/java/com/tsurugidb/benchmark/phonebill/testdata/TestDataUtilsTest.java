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
