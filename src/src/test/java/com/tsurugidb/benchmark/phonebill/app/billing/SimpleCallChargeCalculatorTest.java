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
