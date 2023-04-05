package com.tsurugidb.benchmark.phonebill.testdata;

import org.apache.commons.math3.distribution.RealDistribution;

/**
 * 対数正規分布の乱数発生器を使用して電話番号を選択するPhoneNumberSelector
 *
 */
public class LogNormalPhoneNumberSelector extends AbstractPhoneNumberSelector {
	private static final int SAMPLE_TRY_COUNT = 100;

	/**
	 * 分布関数
	 */
	private RealDistribution distribution;

	/**
	 * 契約のブロックのサイズ
	 */
	int blockSize;


	/**
	 *
	 * @param 使用する分布関数、
	 * @param contractInfoReader 契約情報取得に使用するcontractInfoReaderのインスタンス
	 * @param contracts 契約のリスト、リストの要素は契約そのものではなく何番目の契約かを示す整数値で、
	 *         ランダムな順序になるようにシャッフルされていることを想定している
	 *
	 */
	public LogNormalPhoneNumberSelector(RealDistribution distribution, ContractInfoReader contractInfoReader) {
		super(contractInfoReader);
		blockSize = contractInfoReader.getBlockSize();
		this.distribution = distribution;
	}


	/**
	 * logNormalDistributionを使用して契約を選択する
	 *
	 * @return 何番目の契約か表す整数値
	 */
	@Override
	protected int getContractPos() {
		for(int i =0; i < SAMPLE_TRY_COUNT; i++) {
			double pos = distribution.sample();
			if (pos < blockSize) {
				return (int) pos;
			}
		}
		throw new RuntimeException("Fail to get a random position");
	}
}
