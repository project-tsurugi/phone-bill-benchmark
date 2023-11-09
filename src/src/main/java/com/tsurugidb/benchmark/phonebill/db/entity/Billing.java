package com.tsurugidb.benchmark.phonebill.db.entity;

import java.io.Serializable;
import java.sql.Date;
import java.time.LocalDate;

import com.tsurugidb.benchmark.phonebill.util.DateUtils;

public class Billing implements Serializable {
    /**
    * 電話番号
    */
    private String phoneNumber;

    /**
     * 対象年月
     */
    private long targetMonth;

    /**
     * 基本料金
     */
    private int basicCharge;

    /**
     * 従量料金
     */
    private int meteredCharge;

    /**
    * 請求金額
    */
    private int billingAmount;

    /**
     * バッチ実行ID
     */
    private String batchExecId;

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Billing [phoneNumber=");
        builder.append(phoneNumber);
        builder.append(", targetMonth=");
        builder.append(new Date(targetMonth));
        builder.append(", basicCharge=");
        builder.append(basicCharge);
        builder.append(", meteredCharge=");
        builder.append(meteredCharge);
        builder.append(", billingAmount=");
        builder.append(billingAmount);
        builder.append(", batchExecId=");
        builder.append(batchExecId);
        builder.append("]");
        return builder.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + basicCharge;
        result = prime * result + ((batchExecId == null) ? 0 : batchExecId.hashCode());
        result = prime * result + billingAmount;
        result = prime * result + meteredCharge;
        result = prime * result + ((phoneNumber == null) ? 0 : phoneNumber.hashCode());
        result = prime * result + (int) (targetMonth ^ (targetMonth >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Billing other = (Billing) obj;
        if (basicCharge != other.basicCharge)
            return false;
        if (batchExecId == null) {
            if (other.batchExecId != null)
                return false;
        } else if (!batchExecId.equals(other.batchExecId))
            return false;
        if (billingAmount != other.billingAmount)
            return false;
        if (meteredCharge != other.meteredCharge)
            return false;
        if (phoneNumber == null) {
            if (other.phoneNumber != null)
                return false;
        } else if (!phoneNumber.equals(other.phoneNumber))
            return false;
        if (targetMonth != other.targetMonth)
            return false;
        return true;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public Date getTargetMonth() {
        return new Date(targetMonth);
    }

    public LocalDate getTargetMonthAsLocalDate() {
        return DateUtils.toLocalDate(targetMonth);
    }

    public void setTargetMonth(Date targetMonth) {
        this.targetMonth = targetMonth.getTime();
    }

    public void setTargetMonth(LocalDate targetMonth) {
        this.targetMonth = DateUtils.toEpocMills(targetMonth);
    }

    public int getBasicCharge() {
        return basicCharge;
    }

    public void setBasicCharge(int basicCharge) {
        this.basicCharge = basicCharge;
    }

    public int getMeteredCharge() {
        return meteredCharge;
    }

    public void setMeteredCharge(int meteredCharge) {
        this.meteredCharge = meteredCharge;
    }

    public int getBillingAmount() {
        return billingAmount;
    }

    public void setBillingAmount(int billingAmount) {
        this.billingAmount = billingAmount;
    }

    public String getBatchExecId() {
        return batchExecId;
    }

    public void setBatchExecId(String batchExecId) {
        this.batchExecId = batchExecId;
    }

    public static Billing create(String phoneNumber, String targetMonth, int basicCharge, int meteredCharge, int billingAmount,
            String batchExecId) {
        Billing b = new Billing();
        b.phoneNumber = phoneNumber;
        b.targetMonth = DateUtils.toDate(targetMonth).getTime();
        b.basicCharge = basicCharge;
        b.meteredCharge = meteredCharge;
        b.billingAmount = billingAmount;
        b.batchExecId = batchExecId;
        return b;
    }

}
