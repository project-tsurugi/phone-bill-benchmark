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
package com.tsurugidb.benchmark.phonebill.app.billing;

/**
 * テスト用の単純な月額料金計算クラス.
 * <br>
 * 基本料金3000円、基本料金に2000円分の無料通話分を含む場合の月額料金を計算する。
 * 基本料金の日割り計算は行わない。
 */
public class SimpleBillingCalculator implements BillingCalculator {
	int totalCallCharge = 0;;

	@Override
	public void addCallCharge(int callCharge) {
		totalCallCharge += callCharge;
	}

	@Override
	public int getBasicCharge() {
		return 3000;
	}

	@Override
	public int getMeteredCharge() {
		return totalCallCharge;
	}

	@Override
	public int getBillingAmount() {
		if (totalCallCharge < 2000) {
			return 3000;
		}
		return totalCallCharge + 1000;
	}

	@Override
	public void init() {
		totalCallCharge = 0;
	}
}
