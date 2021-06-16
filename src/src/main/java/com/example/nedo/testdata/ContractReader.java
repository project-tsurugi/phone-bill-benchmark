package com.example.nedo.testdata;

import com.example.nedo.db.Duration;

/**
 * 契約内容を取得するためのインターフェイス
 *
 */
public interface ContractReader {
	/**
	 * @return 契約データの総数
	 */
	int getNumberOfContracts();


	/**
	 * n番目の契約の契約期間を返す
	 *
	 * @param n
	 * @return
	 */
	Duration getDurationByPos(long n);


	/**
	 * n番目の契約の電話番号を返す
	 *
	 * @param n
	 * @return
	 */
	String getPhoneNumberByPos(long n);
}
