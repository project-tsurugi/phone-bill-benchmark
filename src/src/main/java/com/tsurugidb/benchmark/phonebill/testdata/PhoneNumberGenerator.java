package com.tsurugidb.benchmark.phonebill.testdata;

import com.tsurugidb.benchmark.phonebill.app.Config;

/**
 * 電話番号生成器(スレッドアンセーフなので要注意)
 *
 */
public class PhoneNumberGenerator {
	/**
	 * コンフィグレーション
	 */
	private Config config;

	/**
	 * 11桁の電話番号をLONG値で表したときの最大値
	 */
	static final long MAX_PHNE_NUMBER = 99999999999L;

	public PhoneNumberGenerator(Config config) {
		this.config = config;
	}

	/**
	 * 数値から文字列への変換テーブル
	 */
	private static char[] digits = {
			'0', '1', '2', '3', '4',
			'5', '6', '7', '8', '9'
	};


	/**
	 * n番目の電話番号(11桁)を返す
	 *
	 * @param n
	 * @return
	 */
	public String getPhoneNumber(long n) {
		return to11DigtString(getPhoneNumberAsLong(n));
	}

	/**
	 * 電話番号を表すLong値を0パディングされた11桁の文字列に変換する
	 *
	 * @param n
	 * @return
	 */
	String to11DigtString(long n) {
		char buf[] = new char[11];
		int pos = 11;
		while (n > 0) {
			buf[--pos] = digits[(int)(n % 10)];
			n = n / 10;
		}
		while (pos > 0) {
			buf[--pos] = '0';
		}
		return new String(buf);
	}



	public long getPhoneNumberAsLong(long n) {
		if (n < 0 || MAX_PHNE_NUMBER < n) {
			throw new RuntimeException("Out of phone number range: " + n);
		}
		long blockSize = config.duplicatePhoneNumberRate * 2 + config.expirationDateRate + config.noExpirationDateRate;
		long noDupSize = config.expirationDateRate + config.noExpirationDateRate;
		long posInBlock = n % blockSize;
		long phoneNumber = n;
		if (posInBlock >= noDupSize && posInBlock % 2 == 0) {
			phoneNumber = n + 1;
		}
		return phoneNumber;
	}
}
