package com.example.nedo.testdata;

/**
 * 通話履歴のレコード生成時に電話番号を選択するためのインターフェイス
 *
 */
public interface PhoneNumberSelector {
	/**
	 * 指定の通話開始時刻が契約範囲に含まれる電話番号を選択する。
	 * <br>
	 * 発信者電話番号、受信者電話番号の順にこのメソッドを使用して電話番号を選択する。
	 * 発信者電話番号の選択時には、exceptPhoneNumberにnullを指定する。受信者電話番号の
	 * 選択時には、exceptPhoneNumberに発信者電話番号を指定することにより、受信者電話番号と
	 * 発信者電話番号が等しくなるのを避ける。
	 *
	 *
	 * @param startTime 通話開始時刻
	 * @param exceptPhoneNumber 選択しない電話番号。
	 * @return 選択為た電話番号
	 */
	String selectPhoneNumber(long startTime, String exceptPhoneNumber);
}
