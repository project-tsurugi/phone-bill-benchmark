package com.example.nedo.testdata;

import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
	 * 契約数
	 */
	private int numberOfContracts;

	public UniformPhoneNumberSelector(Random random, ContractReader contractReader, int tryCount) {
		super(contractReader,
				IntStream.range(0, contractReader.getNumberOfContracts()).boxed().collect(Collectors.toList()),
				tryCount);
		this.random = random;
		this.numberOfContracts = contractReader.getNumberOfContracts();
	}

	/*
	 * logNormalDistributionを使用して契約を選択する
	 *
	 * @return 何番目の契約か表す整数値
	 */
	@Override
	protected int getContractPos() {
		return TestDataUtils.getRandomInt(random, 0, numberOfContracts);
	}
}
