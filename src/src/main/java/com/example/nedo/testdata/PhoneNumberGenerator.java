package com.example.nedo.testdata;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.example.nedo.app.Config;

/**
 * 電話番号生成器
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
	private static final long MAX_PHNE_NUMBER = 99999999999L;

	/*
	 * 電話番号のlong => Stringの変換結果のキャッシュ
	 */
	private Map<Long, String> phoneNumberCache = new ConcurrentHashMap<Long, String>();



	public PhoneNumberGenerator(Config config) {
		this.config = config;
	}


	/**
	 * n番目の電話番号(11桁)を返す
	 *
	 * @param n
	 * @return
	 */
	String getPhoneNumber(long n) {
		if (n < 0 || MAX_PHNE_NUMBER <= n) {
			throw new RuntimeException("Out of phone number range: " + n);
		}
		String str = phoneNumberCache.get(n);
		if (str != null) {
			return str;
		}
		long blockSize = config.duplicatePhoneNumberRatio * 2 + config.expirationDateRate + config.noExpirationDateRate;
		long noDupSize = config.expirationDateRate + config.noExpirationDateRate;
		long posInBlock = n % blockSize;
		long phoneNumber = n;
		if (posInBlock >= noDupSize && posInBlock % 2 == 0) {
			phoneNumber = n + 1;
		}
		String format = "%011d";
		str = String.format(format, phoneNumber);
		phoneNumberCache.put(n, str);
		return str;
	}
}
