package com.tsurugidb.benchmark.phonebill.app.billing;

public interface BillingCalculator {
	/**
	 * 通話料金を追加する
	 *
	 * @param callChage
	 */
	public void addCallCharge(int callCharge);

	/**
	 * 基本料金を返す
	 */
	public int getBasicCharge();


	/**
	 * 従量料金を返す
	 */
	public int getMeteredCharge();


	/**
	 * 請求金額を返す
	 */
	public int getBillingAmount();

	/**
	 * 初期化する
	 */
	public void init();
}
