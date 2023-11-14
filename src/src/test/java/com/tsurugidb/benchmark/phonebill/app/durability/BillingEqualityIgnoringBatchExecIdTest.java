package com.tsurugidb.benchmark.phonebill.app.durability;

import java.time.LocalDate;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.db.entity.Billing;

public class BillingEqualityIgnoringBatchExecIdTest {

    private Billing billing1;
    private Billing billing2;
    private BillingEqualityIgnoringBatchExecId equalityChecker;

    @BeforeEach
    public void setUp() {
        billing1 = new Billing();
        billing1.setPhoneNumber("123-456-7890");
        billing1.setTargetMonth(LocalDate.of(2023, 4, 1));
        billing1.setBasicCharge(1000);
        billing1.setMeteredCharge(500);
        billing1.setBillingAmount(1500);
        billing1.setBatchExecId("Batch123");

        billing2 = new Billing();
        billing2.setPhoneNumber("123-456-7890");
        billing2.setTargetMonth(LocalDate.of(2023, 4, 1));
        billing2.setBasicCharge(1000);
        billing2.setMeteredCharge(500);
        billing2.setBillingAmount(1500);
        billing2.setBatchExecId("Batch456");

        equalityChecker = new BillingEqualityIgnoringBatchExecId();
    }

    @Test
    public void testBillingEqualityWithSameFields() {
        Assertions.assertTrue(equalityChecker.test(billing1, billing2));
    }

    @Test
    public void testBillingEqualityWithDifferentPhoneNumber() {
        billing2.setPhoneNumber("098-765-4321");
        Assertions.assertFalse(equalityChecker.test(billing1, billing2));
    }

    @Test
    public void testBillingEqualityWithDifferentTargetMonth() {
        billing2.setTargetMonth(LocalDate.of(2023, 5, 1));
        Assertions.assertFalse(equalityChecker.test(billing1, billing2));
    }

    @Test
    public void testBillingEqualityWithDifferentBasicCharge() {
        billing2.setBasicCharge(2000);
        Assertions.assertFalse(equalityChecker.test(billing1, billing2));
    }

    @Test
    public void testBillingEqualityWithDifferentMeteredCharge() {
        billing2.setMeteredCharge(300);
        Assertions.assertFalse(equalityChecker.test(billing1, billing2));
    }

    @Test
    public void testBillingEqualityWithDifferentBillingAmount() {
        billing2.setBillingAmount(1000);
        Assertions.assertFalse(equalityChecker.test(billing1, billing2));
    }

    @Test
    public void testBillingEqualityWithNullBilling1() {
        Assertions.assertFalse(equalityChecker.test(null, billing2));
    }

    @Test
    public void testBillingEqualityWithNullBilling2() {
        Assertions.assertFalse(equalityChecker.test(billing1, null));
    }

    @Test
    public void testBillingEqualityWithBothNull() {
        Assertions.assertFalse(equalityChecker.test(null, null));
    }

    @Test
    public void testBillingEqualityWithSameInstance() {
        Assertions.assertTrue(equalityChecker.test(billing1, billing1));
    }
}

