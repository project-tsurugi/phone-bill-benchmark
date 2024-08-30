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

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class SimpleBillingCalculatorTest {

	@Test
	void test() {
		SimpleBillingCalculator calculator = new SimpleBillingCalculator();

		// 一度も通話がないケース
		assertEquals(3000, calculator.getBillingAmount());
		assertEquals(3000, calculator.getBasicCharge());
		assertEquals(0, calculator.getMeteredCharge());

		// 従量料金が1円のケース
		calculator.addCallCharge(1);
		assertEquals(3000, calculator.getBillingAmount());
		assertEquals(3000, calculator.getBasicCharge());
		assertEquals(1, calculator.getMeteredCharge());

		// 従量料金が1999円のケース
		calculator.addCallCharge(1998);
		assertEquals(3000, calculator.getBillingAmount());
		assertEquals(3000, calculator.getBasicCharge());
		assertEquals(1999, calculator.getMeteredCharge());

		// 従量料金が2000円のケース
		calculator.addCallCharge(1);
		assertEquals(3000, calculator.getBillingAmount());
		assertEquals(3000, calculator.getBasicCharge());
		assertEquals(2000, calculator.getMeteredCharge());

		// 従量料金が2001円のケース
		calculator.addCallCharge(1);
		assertEquals(3001, calculator.getBillingAmount());
		assertEquals(3000, calculator.getBasicCharge());
		assertEquals(2001, calculator.getMeteredCharge());

		// 従量料金が2999円のケース
		calculator.addCallCharge(998);
		assertEquals(3999, calculator.getBillingAmount());
		assertEquals(3000, calculator.getBasicCharge());
		assertEquals(2999, calculator.getMeteredCharge());

		// 従量料金が3000円のケース
		calculator.addCallCharge(1);
		assertEquals(4000, calculator.getBillingAmount());
		assertEquals(3000, calculator.getBasicCharge());
		assertEquals(3000, calculator.getMeteredCharge());

		// 従量料金が3001円のケース
		calculator.addCallCharge(1);
		assertEquals(4001, calculator.getBillingAmount());
		assertEquals(3000, calculator.getBasicCharge());
		assertEquals(3001, calculator.getMeteredCharge());

		// 従量料金が6001円のケース
		calculator.addCallCharge(3000);
		assertEquals(7001, calculator.getBillingAmount());
		assertEquals(3000, calculator.getBasicCharge());
		assertEquals(6001, calculator.getMeteredCharge());
	}

}
