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
