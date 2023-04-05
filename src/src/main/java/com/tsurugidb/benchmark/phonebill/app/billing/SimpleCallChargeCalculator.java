package com.tsurugidb.benchmark.phonebill.app.billing;

/**
 * テスト用単純な通話料金計算クラス.
 */
public class SimpleCallChargeCalculator implements CallChargeCalculator {

	/**
	 * 通話時間が１分未満の場合、通話料金は10円、１分以上の場合、１分10円
	 * １分に満たない通話時間は切り上げて計算する。
	 */
	@Override
	public int calc(int time) {
		if (time < 60) {
			return 10;
		}
		return ((time - 1) / 60 + 1) * 10;
	}

}
