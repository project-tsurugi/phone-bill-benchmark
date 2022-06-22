package com.example.nedo.app.billing;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Date;

import org.junit.jupiter.api.Test;

import com.example.nedo.db.old.Contract;
import com.example.nedo.db.old.DBUtils;

class CalculationTargetTest {



	@Test
	final void testGetEndOfTask() {
		CalculationTarget target = CalculationTarget.getEndOfTask();
		assertTrue(target.isEndOfTask());
	}

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
	final void testIsEndOfTask() {
		assertTrue(new CalculationTarget(null, null, null, null, null, true).isEndOfTask());
		assertFalse(new CalculationTarget(null, null, null, null, null, false).isEndOfTask());
	}

	@Test
	final void testGetStart() {
		Date date = DBUtils.toDate("2021-05-15 13:24:35");
		assertEquals(date, new CalculationTarget(null, null, null, date, null, false).getStart());
		assertEquals(null, new CalculationTarget(null, null, null, null, null, false).getStart());
	}

	@Test
	final void testGetEnd() {
		Date date = DBUtils.toDate("2021-05-15 13:24:35");
		assertEquals(date, new CalculationTarget(null, null, null, null, date, false).getEnd());
		assertEquals(null, new CalculationTarget(null, null, null, null, null, false).getEnd());
	}

}
