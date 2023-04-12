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
