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
