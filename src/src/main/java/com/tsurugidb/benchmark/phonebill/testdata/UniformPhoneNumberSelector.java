package com.tsurugidb.benchmark.phonebill.testdata;

import java.util.Random;

/**
 * 一様分布の乱数発生器を使用して電話番号を選択するPhoneNumberSelector
 *
 */
public class UniformPhoneNumberSelector extends AbstractPhoneNumberSelector {
	/**
	 * 乱数生成器
	 */
	private Random random;

	/**
	 * 契約のブロックのサイズ
	 */
	int blockSize;

	public UniformPhoneNumberSelector(Random random, ContractInfoReader contractInfoReader) {
		super(contractInfoReader);
		this.random = random;
		blockSize = contractInfoReader.getBlockSize();
	}

	/**
	 * ランダムな契約を取得する
	 *
	 * @return 何番目の契約かを表す整数値
	 */
	@Override
	protected int getContractPos() {
		return random.nextInt(blockSize);
	}
}
