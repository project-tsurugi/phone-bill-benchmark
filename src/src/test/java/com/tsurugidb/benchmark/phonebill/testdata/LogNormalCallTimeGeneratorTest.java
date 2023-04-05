package com.tsurugidb.benchmark.phonebill.testdata;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.app.Config;

class LogNormalCallTimeGeneratorTest {

	/**
	 * etTimeSecs()のテスト
	 *
	 * @throws IOException
	 */
	@Test
	void testGetTimeSecs() throws IOException {
		int maxCallTimeSecs = 3600;
		Config config = Config.getConfig();
		config.maxCallTimeSecs = maxCallTimeSecs;
		LogNormalCallTimeGenerator generator = new LogNormalCallTimeGenerator(config);

		// 生成される値が1～maxCallTimeSecsの範囲であることを確認する
		int max = Integer.MIN_VALUE;
		int min = Integer.MAX_VALUE;

		for (int i = 0; i < 1000000; i++) {
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

		// 分布に偏りがあることを確認

		int underHalf = 0;
		int overHalf = 0;
		int loopCount = 1000000;
		for (int i = 0; i < loopCount; i++) {
			int time = generator.getTimeSecs();
			if ( time <= maxCallTimeSecs / 2) {
				underHalf++;
			} else {
				overHalf++;
			}
		}
		System.out.println("underHalf = " + underHalf +  ", overHalf = " + overHalf);
		assertTrue(underHalf > loopCount * 0.9);
		assertTrue(overHalf < loopCount * 0.1);

	}

}
