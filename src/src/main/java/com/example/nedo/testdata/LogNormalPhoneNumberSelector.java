package com.example.nedo.testdata;

import java.util.List;

import org.apache.commons.math3.distribution.RealDistribution;

/**
 * 対数正規分布の乱数発生器を使用して電話番号を選択するPhoneNumberSelector
 *
 */
public class LogNormalPhoneNumberSelector extends AbstractPhoneNumberSelector {
	private static final int TRY_COUNT = 100;

	/**
	 * 分布関数
	 */
	private RealDistribution distribution;

	/**
	 * 契約数
	 */
	private int numberOfContracts;


	/**
	 *
	 * @param 使用する分布関数、
	 * @param contractReader 契約情報取得に使用するcontractReaderのインスタンス
	 * @param contracts 契約のリスト、リストの要素は契約そのものではなく何番目の契約かを示す整数値で、
	 *         ランダムな順序になるようにシャッフルされていることを想定している
	 *
	 */
	public LogNormalPhoneNumberSelector(RealDistribution distribution, ContractReader contractReader,
			List<Integer> contracts) {
		super(contractReader, contracts, contractReader.getNumberOfContracts());
		this.distribution = distribution;
		this.numberOfContracts = contractReader.getNumberOfContracts();
	}


	/**
	 * logNormalDistributionを使用して契約を選択する
	 *
	 * @return 何番目の契約か表す整数値
	 */
	@Override
	protected int getContractPos() {
		for(int i =0; i < TRY_COUNT; i++) {
			double pos = distribution.sample();
			if (pos < numberOfContracts) {
				return (int) pos;
			}
		}
		throw new RuntimeException("Fail to get a random position");
	}

}
