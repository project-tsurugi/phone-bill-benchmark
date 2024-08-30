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
 * テスト用単純な通話料金計算クラス.
 */
public class SimpleCallChargeCalculator implements CallChargeCalculator {

	/**
	 * 通話時間が１分未満の場合、通話料金は10円、１分以上の場合、１分10円
	 * １分に満たない通話時間は切り上げて計算する。
	 */
	@Override
	public int calc(int time) {
		if (time < 60) {
			return 10;
		}
		return ((time - 1) / 60 + 1) * 10;
	}

}
