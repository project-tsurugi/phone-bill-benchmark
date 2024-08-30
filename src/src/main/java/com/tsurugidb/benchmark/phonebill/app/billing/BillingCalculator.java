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

public interface BillingCalculator {
	/**
	 * 通話料金を追加する
	 *
	 * @param callChage
	 */
	public void addCallCharge(int callCharge);

	/**
	 * 基本料金を返す
	 */
	public int getBasicCharge();


	/**
	 * 従量料金を返す
	 */
	public int getMeteredCharge();


	/**
	 * 請求金額を返す
	 */
	public int getBillingAmount();

	/**
	 * 初期化する
	 */
	public void init();
}
