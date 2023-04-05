package com.tsurugidb.benchmark.phonebill.testdata;

import java.util.Random;

/**
 * 一様分布の通話時間生成器
 *
 */
public class UniformCallTimeGenerator implements CallTimeGenerator {
	/**
	 * 乱数生成器
	 */
	Random random;

	/**
	 * 通話時間の最大値
	 */
	int maxCallTimeSecs;

	public UniformCallTimeGenerator(Random random, int maxCallTimeSecs) {
		this.random = random;
		this.maxCallTimeSecs = maxCallTimeSecs;
	}

	@Override
	public int getTimeSecs() {
		return random.nextInt(maxCallTimeSecs) + 1;
	}
}
