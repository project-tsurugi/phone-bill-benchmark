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
