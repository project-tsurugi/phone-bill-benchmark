package com.tsurugidb.benchmark.phonebill.testdata;

import com.tsurugidb.benchmark.phonebill.db.jdbc.Duration;

/**
 * 通話履歴のレコード生成時に電話番号を選択するための抽象クラス、
 * 使用する乱数生成器に合わせた具象クラスを実装する
 */
public abstract class  AbstractPhoneNumberSelector implements PhoneNumberSelector{
	/**
	 * 契約情報取得に使用するcontractInfoReaderのインスタンス
	 */
	private ContractInfoReader contractInfoReader;

	/**
	 * コンストラクタ
	 *
	 * @param contractInfoReader
	 * @param contracts
	 * @param tryCount
	 */
	public AbstractPhoneNumberSelector(ContractInfoReader contractInfoReader) {
		this.contractInfoReader = contractInfoReader;
	}

	/**
	 * 指定の通話開始時刻が契約範囲に含まれる電話番号を選択する。
	 */
	@Override
	public long selectPhoneNumber(long startTime, long exceptPhoneNumber) {
		int blockSize = contractInfoReader.getBlockSize();
		int pos = getContractPos();
		for(int i =0; i < blockSize; i++) {
			long n = contractInfoReader.getRandomN(pos);
			long phoneNumber = contractInfoReader.getPhoneNumberAsLong(n);
			if (exceptPhoneNumber != phoneNumber) {
				Duration d = contractInfoReader.getInitialDuration(n);
				if (d.end == null) {
					if (d.start <= startTime) {
						return phoneNumber;
					}
				} else {
					if (d.start <= startTime && startTime < d.end) {
						return phoneNumber;
					}
				}
			}
			// この位置が有効な契約でない場合は有効な契約が見つかるまで順次調べる
			if (++pos >= blockSize) {
				pos = 0;
			}
		}
		throw new RuntimeException("Not found! start time = " + new java.util.Date(startTime));
	}


	/**
	 * HistoryInsertAppが生成する履歴データに有効な電話番号を選択する。
	 */
	@Override
	public long selectPhoneNumber(long exceptPhoneNumber) {
		int blockSize = contractInfoReader.getBlockSize();
		int pos = getContractPos();
		// どの契約が有効か分かっているので、有効な契約のリストから選択すれば1回で有効な契約を
		// 選択可能だが、一様分布でない乱数を使用する場合に、selectPhoneNumber(long startTime, long exceptPhoneNumber)
		// と選択する電話番号の分布密度が一致するようにあえてこのような処理をしている。
		for(int i =0; i < blockSize; i++) {
			long n = contractInfoReader.getRandomN(pos);
			long phoneNumber = contractInfoReader.getPhoneNumberAsLong(n);
			if (exceptPhoneNumber != phoneNumber && contractInfoReader.isActive(n)) {
				return phoneNumber;
			}
			// この位置が有効な契約でない場合は有効な契約が見つかるまで順次調べる
			if (++pos >= blockSize) {
				pos = 0;
			}
		}
		// ブロック上のすべての契約が無効だった場合
		throw new RuntimeException("Not found!");
	}


	/**
	 * ランダムな契約を取得する
	 *
	 * @return 何番目の契約かを表す整数値
	 */
	protected abstract int getContractPos();
}
