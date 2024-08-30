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

class SimpleCallChargeCalculatorTest {

	@Test
	void test() {
		SimpleCallChargeCalculator calculator = new SimpleCallChargeCalculator();
		assertEquals(10, calculator.calc(-1));
		assertEquals(10, calculator.calc(0));
		assertEquals(10, calculator.calc(59));
		assertEquals(10, calculator.calc(60));
		assertEquals(20, calculator.calc(61));
		assertEquals(20, calculator.calc(119));
		assertEquals(20, calculator.calc(120));
		assertEquals(30, calculator.calc(121));
	}

}
