package com.tsurugidb.benchmark.phonebill.testdata;

import java.util.Random;

import org.apache.commons.math3.distribution.LogNormalDistribution;
import org.apache.commons.math3.distribution.RealDistribution;

import com.tsurugidb.benchmark.phonebill.app.Config.DistributionFunction;

/**
 * 通話履歴のレコード生成時に電話番号を選択するためセレクタ
 *
 */
public interface PhoneNumberSelector {
	/**
	 * 指定の通話開始時刻が契約範囲に含まれる電話番号を選択する。
	 * <br>
	 * 発信者電話番号、受信者電話番号の順にこのメソッドを使用して電話番号を選択する。
	 * 発信者電話番号の選択時には、exceptPhoneNumberに-1を指定する。受信者電話番号の
	 * 選択時には、exceptPhoneNumberに発信者電話番号を指定することにより、受信者電話番号と
	 * 発信者電話番号が等しくなるのを避ける。電話番号には文字列ではなく何番目の電話番号かを表す
	 * long値を使用する。
	 *
	 *
	 * @param startTime 通話開始時刻
	 * @param exceptPhoneNumber 選択しない電話番号。
	 * @return 選択した電話番号
	 */
	long selectPhoneNumber(long startTime, long exceptPhoneNumber);

	/**
	 * HistoryInsertAppが生成する履歴データに有効な電話番号を選択する。
	 * <br>
	 * HistoryInserApp用のメソッド。HistoryInsertAppが生成する履歴レコードのstartTimeに有効な
	 * 契約を選択する。
	 * 発信者電話番号、受信者電話番号の順にこのメソッドを使用して電話番号を選択する。
	 * 発信者電話番号の選択時には、exceptPhoneNumberに-1を指定する。受信者電話番号の
	 * 選択時には、exceptPhoneNumberに発信者電話番号を指定することにより、受信者電話番号と
	 * 発信者電話番号が等しくなるのを避ける。電話番号には文字列ではなく何番目の電話番号かを表す
	 * long値を使用する。
	 *
	 *
	 * @param exceptPhoneNumber 選択しない電話番号。
	 * @return 選択した電話番号
	 */
	long selectPhoneNumber(long exceptPhoneNumber);

	/**
	 * セレクタを生成する
	 *
	 * @param random 使用する乱数生成器
	 * @param distributionFunction 使用する分布関数
	 * @param scale 分布関数に対数正規分布を使用するときのscale値
	 * @param shape 分布関数に対数正規分布を使用するときのshape値
	 * @param contractInfoReader 使用するcontractInfoReader
	 * @return
	 */
	public static PhoneNumberSelector createSelector(Random random,
			DistributionFunction distributionFunction, double scale, double shape,
			ContractInfoReader contractInfoReader) {
		PhoneNumberSelector phoneNumberSelector = null;
		switch (distributionFunction) {
		case LOGNORMAL:
			RealDistribution distribution = new LogNormalDistribution(scale, shape);
			distribution.reseedRandomGenerator(random.nextLong());
			phoneNumberSelector =  new LogNormalPhoneNumberSelector(distribution, contractInfoReader);
			break;
		case UNIFORM:
			phoneNumberSelector =  new UniformPhoneNumberSelector(random, contractInfoReader);
			break;
		}
		return phoneNumberSelector;
	}
}
