package com.tsurugidb.benchmark.phonebill.testdata;

import org.apache.commons.math3.distribution.LogNormalDistribution;

import com.tsurugidb.benchmark.phonebill.app.Config;

/**
 * 対数正規分布の通話時間生成器
 *
 */
public class LogNormalCallTimeGenerator implements CallTimeGenerator {
	/**
	 * 乱数生成器
	 */
	private LogNormalDistribution logNormalDistribution;

	/**
	 * 通話時間の最大値
	 */
	private int maxCallTimeSecs;

	public LogNormalCallTimeGenerator(Config config) {
		this.maxCallTimeSecs = config.maxCallTimeSecs;
		logNormalDistribution = new LogNormalDistribution(config.callTimeScale, config.callTimeShape);
		logNormalDistribution.reseedRandomGenerator(config.randomSeed);
	}

	@Override
	public int getTimeSecs() {
		int timeSecs;
		do {
			timeSecs = (int)logNormalDistribution.sample() + 1;
		} while (timeSecs > maxCallTimeSecs);
		return timeSecs;
	}
}
