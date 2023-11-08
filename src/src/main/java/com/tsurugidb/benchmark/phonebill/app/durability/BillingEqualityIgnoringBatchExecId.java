package com.tsurugidb.benchmark.phonebill.app.durability;

import java.util.function.BiPredicate;

import com.tsurugidb.benchmark.phonebill.db.entity.Billing;

public class BillingEqualityIgnoringBatchExecId implements BiPredicate<Billing, Billing> {
    @Override
    public boolean test(Billing billing1, Billing billing2) {
        if (billing1 == null || billing2 == null) {
            return false;
        }
        if (billing1 == billing2) {
            return true;
        }
        return (billing1.getPhoneNumber().equals(billing2.getPhoneNumber())) &&
               (billing1.getTargetMonth().equals(billing2.getTargetMonth())) &&
               (billing1.getBasicCharge() == billing2.getBasicCharge()) &&
               (billing1.getMeteredCharge() == billing2.getMeteredCharge()) &&
               (billing1.getBillingAmount() == billing2.getBillingAmount());
    }
}
