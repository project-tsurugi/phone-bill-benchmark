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
