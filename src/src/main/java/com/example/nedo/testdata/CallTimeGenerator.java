package com.example.nedo.testdata;

import java.util.Random;

import com.example.nedo.app.Config;

/**
 * 通話時間生成器
 *
 */
public interface CallTimeGenerator {

	/**
	 * 通話時間を生成する
	 */
	int getTimeSecs();


	/**
	 * 通話時間生成器を作成する
	 *
	 * @param random 特定の通話時間生成器が使用する乱数生成器
	 * @param config
	 * @return
	 */
	public static CallTimeGenerator createCallTimeGenerator(Random random, Config config) {
		switch (config.callTimeDistribution) {
		case LOGNORMAL:
			return new LogNormalCallTimeGenerator(config);
		case UNIFORM:
			return new UniformCallTimeGenerator(random, config.maxCallTimeSecs);
		default:
			throw new AssertionError(config.callTimeDistribution.name());
		}
	}
}
