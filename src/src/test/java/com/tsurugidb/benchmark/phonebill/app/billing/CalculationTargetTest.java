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

import java.sql.Date;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;

class CalculationTargetTest {



	@Test
	final void testGetContract() {
		Contract c = new Contract();
		CalculationTarget target = new CalculationTarget(c, null, null, null, null, false);
		assertEquals(c, target.getContract());
	}

	@Test
	final void testGetBillingCalculator() {
		BillingCalculator b = new SimpleBillingCalculator();
		CalculationTarget target = new CalculationTarget(null, b, null, null, null, false);
		assertEquals(b, target.getBillingCalculator());
	}

	@Test
	final void testGetCallChargeCalculator() {
		CallChargeCalculator c = new SimpleCallChargeCalculator();
		CalculationTarget target = new CalculationTarget(null, null, c, null, null, false);
		assertEquals(c, target.getCallChargeCalculator());
	}

	@Test
	final void testGetStart() {
		Date date = DateUtils.toDate("2021-05-15");
		assertEquals(date, new CalculationTarget(null, null, null, date, null, false).getStart());
		assertEquals(null, new CalculationTarget(null, null, null, null, null, false).getStart());
	}

	@Test
	final void testGetEnd() {
		Date date = DateUtils.toDate("2021-05-15");
		assertEquals(date, new CalculationTarget(null, null, null, null, date, false).getEnd());
		assertEquals(null, new CalculationTarget(null, null, null, null, null, false).getEnd());
	}

}
