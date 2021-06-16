package com.example.nedo.testdata;

import java.util.Random;

import com.example.nedo.db.Duration;

/**
 * 一様分布の乱数発生器を使用して電話番号を選択するPhoneNumberSelector
 *
 */
public class UniformPhoneNumberSelector implements PhoneNumberSelector {
	/**
	 * 乱数生成器
	 */
	private Random random;


	/**
	 * 契約情報取得
	 */
	private ContractReader contractReader;


	/**
	 * 選択した契約が通話開始時刻に無効な契約だった場合に、違う契約を選択する回数
	 */
	private int tryCount;



	public UniformPhoneNumberSelector(Random random, ContractReader contractReader, int tryCount) {
		this.random = random;
		this.contractReader = contractReader;
		this.tryCount = tryCount;
	}



	@Override
	public String selectPhoneNumber(long startTime, String exceptPhoneNumber) {
		long pos = 	TestDataUtils.getRandomLong(random, 0, contractReader.getNumberOfContracts());

		int c = 0;
		for (;;) {
			if (exceptPhoneNumber == null || !exceptPhoneNumber.equals(contractReader.getPhoneNumberByPos(pos))) {
				Duration d = contractReader.getDurationByPos(pos);
				if (d.end == null) {
					if (d.start.getTime() <= startTime) {
						break;
					}
				} else {
					if (d.start.getTime() <= startTime && startTime < d.end.getTime()) {
						break;
					}
				}
			}
			pos++;
			if (pos >= contractReader.getNumberOfContracts()) {
				pos = 0;
			}
			if (++c >= tryCount) {
				throw new RuntimeException("Not found! start time = " + new java.util.Date(startTime));
			}
		}
		return contractReader.getPhoneNumberByPos(pos);
	}

}
