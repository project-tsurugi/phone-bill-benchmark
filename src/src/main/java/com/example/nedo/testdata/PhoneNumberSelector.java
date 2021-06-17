package com.example.nedo.testdata;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.math3.distribution.LogNormalDistribution;
import org.apache.commons.math3.distribution.RealDistribution;

import com.example.nedo.app.Config.DistributionFunction;

/**
 * 通話履歴のレコード生成時に電話番号を選択するためセレクタ
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

	/**
	 * セレクタを生成する
	 *
	 * @param random 使用する乱数生成器
	 * @param distributionFunction 使用する分布関数
	 * @param scale 分布関数に対数正規分布を使用するときのscale値
	 * @param shape 分布関数に対数正規分布を使用するときのshape値
	 * @param contractReader 使用するcontractReader
	 * @param tryCount 指定時刻に有効な契約を選択できるまでの契約選択の最大試行回数
	 * @return
	 */
	public static PhoneNumberSelector createSelector(Random random,
			DistributionFunction distributionFunction,
			double scale, double shape,
			ContractReader contractReader,
			int tryCount) {
		switch (distributionFunction) {
		case LOGNORMAL:
			RealDistribution distribution = new LogNormalDistribution(scale, shape);
			distribution.reseedRandomGenerator(random.nextLong());
			List<Integer> contracts = IntStream.range(0, contractReader.getNumberOfContracts()).boxed()
					.collect(Collectors.toList());
			Collections.shuffle(contracts);
			return new LogNormalPhoneNumberSelector(distribution, contractReader, contracts);
		case UNIFORM:
			return new UniformPhoneNumberSelector(random, contractReader, tryCount);
		default:
			throw new AssertionError(distributionFunction.name());
		}
	}
}
