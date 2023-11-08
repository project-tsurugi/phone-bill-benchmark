package com.tsurugidb.benchmark.phonebill.app.durability;

import java.util.List;

import com.tsurugidb.benchmark.phonebill.db.entity.Billing;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.History;

public class DatabaseSnapshot {
    private List<History> histories;
    private List<Contract> contracts;
    private List<Billing> billings;

    public DatabaseSnapshot(List<History> histories, List<Contract> contracts, List<Billing> billings) {
        this.histories = histories;
        this.contracts = contracts;
        this.billings = billings;
    }

    public List<History> getHistories() {
        return histories;
    }

    public void setHistories(List<History> histories) {
        this.histories = histories;
    }

    public List<Contract> getContracts() {
        return contracts;
    }

    public void setContracts(List<Contract> contracts) {
        this.contracts = contracts;
    }

    public List<Billing> getBillings() {
        return billings;
    }

    public void setBillings(List<Billing> billings) {
        this.billings = billings;
    }
}