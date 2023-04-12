package com.tsurugidb.benchmark.phonebill.app.billing;

/**
 * テスト用の単純な月額料金計算クラス.
 * <br>
 * 基本料金3000円、基本料金に2000円分の無料通話分を含む場合の月額料金を計算する。
 * 基本料金の日割り計算は行わない。
 */
public class SimpleBillingCalculator implements BillingCalculator {
	int totalCallCharge = 0;;

	@Override
	public void addCallCharge(int callCharge) {
		totalCallCharge += callCharge;
	}

	@Override
	public int getBasicCharge() {
		return 3000;
	}

	@Override
	public int getMeteredCharge() {
		return totalCallCharge;
	}

	@Override
	public int getBillingAmount() {
		if (totalCallCharge < 2000) {
			return 3000;
		}
		return totalCallCharge + 1000;
	}

	@Override
	public void init() {
		totalCallCharge = 0;
	}
}
