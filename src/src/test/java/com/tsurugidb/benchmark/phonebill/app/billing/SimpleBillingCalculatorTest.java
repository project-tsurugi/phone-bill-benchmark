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
