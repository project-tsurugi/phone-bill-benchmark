package com.example.nedo.testdata;

import java.util.List;

import com.example.nedo.db.Duration;

/**
 * 通話履歴のレコード生成時に電話番号を選択するための抽象クラス、
 * 使用する乱数生成器に合わせた具象クラスを実装する
 */
public abstract class  AbstractPhoneNumberSelector implements PhoneNumberSelector{
	/**
	 * 契約情報取得に使用するcontractReaderのインスタンス
	 */
	private ContractReader contractReader;

	/**
	 * 契約のリスト、リストの要素は契約そのものではなく何番目の契約かを示す整数値.
	 * 非一様分布な乱数生成器を使用する場合に、契約をシャッフルするために使用する。
	 */
	private List<Integer> contracts;

	/**
	 * 選択した契約が通話開始時刻に無効な契約だった場合に、違う契約を選択する回数
	 */
	private int tryCount;

	/**
	 * コンストラクタ
	 *
	 * @param contractReader
	 * @param contracts
	 * @param tryCount
	 */
	public AbstractPhoneNumberSelector(ContractReader contractReader, List<Integer> contracts, int tryCount) {
		this.contractReader = contractReader;
		this.contracts = contracts;
		this.tryCount = tryCount;
	}

	@Override
	public long selectPhoneNumber(long startTime, long exceptPhoneNumber) {
		int pos = getContractPos();
		for(int i =0; i < tryCount; i++) {
			int shuffledPos = contracts.get(pos);
			if (exceptPhoneNumber != shuffledPos) {
				Duration d = contractReader.getDurationByPos(shuffledPos);
				if (d.end == null) {
					if (d.start <= startTime) {
						return shuffledPos;
					}
				} else {
					if (d.start <= startTime && startTime < d.end) {
						return shuffledPos;
					}
				}
			}
			pos++;
			if (pos >= contracts.size()) {
				pos = 0;
			}
		}
		throw new RuntimeException("Not found! start time = " + new java.util.Date(startTime));
	}

	/**
	 * ランダムな契約を取得する
	 *
	 * @return 何番目の契約かを表す整数値
	 */
	protected abstract int getContractPos();
}
