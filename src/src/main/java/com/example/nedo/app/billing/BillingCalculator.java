package com.example.nedo.app.billing;

public interface BillingCalculator {
	/**
	 * 通話料金を追加する
	 *
	 * @param callChage
	 */
	void addCallCharge(int callCharge);

	/**
	 * 基本料金を返す
	 */
	int getBasicCharge();


	/**
	 * 従量料金を返す
	 */
	int getMeteredCharge();


	/**
	 * 請求金額を返す
	 */
	int getBillingAmount();
}
