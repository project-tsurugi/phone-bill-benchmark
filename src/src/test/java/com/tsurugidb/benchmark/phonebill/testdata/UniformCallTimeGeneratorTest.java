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

import java.io.IOException;
import java.util.Random;

import org.junit.jupiter.api.Test;

class UniformCallTimeGeneratorTest {

	/**
	 * etTimeSecs()のテスト
	 *
	 * @throws IOException
	 */
	@Test
	void testGetTimeSecs() throws IOException {
		int maxCallTimeSecs = 3600;
		Random random = new Random();
		UniformCallTimeGenerator generator = new UniformCallTimeGenerator(random, maxCallTimeSecs);

		// 生成される値が1～maxCallTimeSecsの範囲であることを確認する
		int max = Integer.MIN_VALUE;
		int min = Integer.MAX_VALUE;

		for (int i = 0; i < 10000000; i++) {
			int time = generator.getTimeSecs();
			if (time < min) {
				min = time;
			}
			if (time > max) {
				max = time;
			}
		}
		assertEquals(1, min);
		assertEquals(maxCallTimeSecs, max);

		// 分布に偏りがないことを確認

		int underHalf = 0;
		int overHalf = 0;
		int loopCount = 10000000;
		for (int i = 0; i < loopCount; i++) {
			int time = generator.getTimeSecs();
			if ( time <= maxCallTimeSecs / 2) {
				underHalf++;
			} else {
				overHalf++;
			}
		}
		System.out.println("underHalf = " + underHalf +  ", overHalf = " + overHalf);
		assertTrue(loopCount * 0.499 < underHalf && underHalf < loopCount * 0.501);
		assertTrue(loopCount * 0.499 < overHalf && overHalf < loopCount * 0.501);
	}

}
