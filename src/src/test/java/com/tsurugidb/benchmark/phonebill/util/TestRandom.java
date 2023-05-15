package com.tsurugidb.benchmark.phonebill.util;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * テスト用のRandomクラス
 * <p>
 * nextInt()が返す値をsetValues()で指定できる。
 */
@SuppressFBWarnings("SE_NO_SERIALVERSIONID")
public class TestRandom extends Random {
	Queue<Integer> queue = new LinkedList<>();

	public void setValues(Integer... integers) {
		queue.clear();
		queue.addAll(Arrays.asList(integers));
	}

	@Override
	public int nextInt() {
		return queue.poll();
	}

	@Override
	public int nextInt(int bound) {
		return nextInt();
	}
}