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
 * テストデータ生成に使用するUtilクラス
 *
 */
public interface TestDataUtils {
	/**
	 * 指定の乱数生成器を使用してmin以上max未満のランダムなlong値を取得する
	 *
	 * @param min
	 * @param max
	 * @return
	 */
	public static long getRandomLong(Random random, long min, long max) {
		return min + (long) (random.nextDouble() * (max - min));
	}

	/**
	 * 指定の乱数生成器を使用してmin以上max未満のランダムなint値を取得する
	 *
	 * @param min
	 * @param max
	 * @return
	 */
	public static int getRandomInt(Random random, int min, int max) {
		return min + (int) (random.nextDouble() * (max - min));
	}
}
