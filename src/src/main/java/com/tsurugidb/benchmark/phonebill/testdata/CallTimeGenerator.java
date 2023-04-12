package com.tsurugidb.benchmark.phonebill.testdata;

import java.util.Random;

import com.tsurugidb.benchmark.phonebill.app.Config;

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
		CallTimeGenerator generator = null;
		switch (config.callTimeDistribution) {
		case LOGNORMAL:
			generator = new LogNormalCallTimeGenerator(config);
			break;
		case UNIFORM:
			generator = new UniformCallTimeGenerator(random, config.maxCallTimeSecs);
			break;
		}
		return generator;
	}
}
