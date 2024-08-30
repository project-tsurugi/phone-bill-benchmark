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
