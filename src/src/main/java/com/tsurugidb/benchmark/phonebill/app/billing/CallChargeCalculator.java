package com.tsurugidb.benchmark.phonebill.app.billing;

public interface CallChargeCalculator {
	/**
	 * 通話時間から通話料金を計算する
	 *
	 * @param time 通話時間(秒)
	 * @return 通話料金
	 */
	int calc(int time);
}
