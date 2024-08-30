/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
