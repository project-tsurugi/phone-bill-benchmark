package com.tsurugidb.benchmark.phonebill.db.entity;

import java.io.Serializable;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;

import com.tsurugidb.benchmark.phonebill.util.DateUtils;

public class Contract implements Cloneable, Serializable {
    /**
     * 電話番号
     */
    private String phoneNumber;

    /**
     * 契約開始日
     */
    private long startDate;

    /**
     * 契約終了日
     */
    private Long endDate; // Null可のフィールドのためlongでなくLongを使用する。

    /**
     * 料金計算ルール
     */
    private String rule;

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Contract [phone_number=");
        builder.append(phoneNumber);
        builder.append(", start_date=");
        builder.append(new Date(startDate));
        builder.append(", end_date=");
        builder.append(endDate == null ? "(null)": new Date(endDate));
        builder.append(", rule=");
        builder.append(rule);
        builder.append("]");
        return builder.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((endDate == null) ? 0 : endDate.hashCode());
        result = prime * result + ((phoneNumber == null) ? 0 : phoneNumber.hashCode());
        result = prime * result + ((rule == null) ? 0 : rule.hashCode());
        result = prime * result + (int) (startDate ^ (startDate >>> 32));
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
        Contract other = (Contract) obj;
        if (endDate == null) {
            if (other.endDate != null)
                return false;
        } else if (!endDate.equals(other.endDate))
            return false;
        if (phoneNumber == null) {
            if (other.phoneNumber != null)
                return false;
        } else if (!phoneNumber.equals(other.phoneNumber))
            return false;
        if (rule == null) {
            if (other.rule != null)
                return false;
        } else if (!rule.equals(other.rule))
            return false;
        if (startDate != other.startDate)
            return false;
        return true;
    }

    @Override
    public Contract clone()  {
        try {
            return (Contract) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new InternalError(e);
        }
    }

    public Key getKey() {
        return createKey(phoneNumber, startDate);
    }


    /**
     * Contractsの主キー
     */
    public static class Key {
        private String phoneNumber;
        private long startDate;

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((phoneNumber == null) ? 0 : phoneNumber.hashCode());
            result = prime * result + (int) (startDate ^ (startDate >>> 32));
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
            Key other = (Key) obj;
            if (phoneNumber == null) {
                if (other.phoneNumber != null)
                    return false;
            } else if (!phoneNumber.equals(other.phoneNumber))
                return false;
            if (startDate != other.startDate)
                return false;
            return true;
        }

        public String getPhoneNumber() {
            return phoneNumber;
        }

        public LocalDate getStDateAsLocalDate() {
            return DateUtils.toLocalDate(startDate);
        }

        public void setPhoneNumber(String phoneNumber) {
            this.phoneNumber = phoneNumber;
        }

        public Date getStartDate() {
            return new Date(startDate);
        }

        public LocalDateTime getStartDateAsLocalDateTime() {
            return DateUtils.toLocalDateTime(startDate);
        }

        public LocalDate getStartDateAsLocalDate() {
            return DateUtils.toLocalDate(startDate);
        }

        public long getStartDateAsLong() {
            return startDate;
        }

        public void setStartDate(Date startDate) {
            this.startDate = startDate.getTime();
        }

        public void setStartDate(LocalDate localDate) {
            this.startDate = DateUtils.toEpocMills(localDate);
        }
    }

    /**
     * 電話番号と契約開始日を指定してKeyを生成する
     *
     * @param phoneNumber
     * @param startDate
     * @return
     */
    public static Key createKey(String phoneNumber, long startDate) {
        Key key = new Key();
        key.phoneNumber = phoneNumber;
        key.startDate = startDate;
        return key;
    }


    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public Date getStartDate() {
        return new Date(startDate);
    }

    public LocalDate getStartDateAsLocalDate() {
        return DateUtils.toLocalDate(startDate);
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate.getTime();
    }

    public void setStartDate(LocalDate startDate) {
        this.startDate = DateUtils.toEpocMills(startDate);
    }

    public void setStartDate(long startDate) {
        this.startDate = startDate;
    }

    public Date getEndDate() {
        return endDate == null ? null : new Date(endDate);
    }

    // Tsurugiがis not nullを未サポートなのでNULL代わりにLocalDate.MAXを使う。
    public LocalDate getEndDateAsLocalDate() {
        if (endDate == null) {
            return LocalDate.MAX;
        }
        return DateUtils.toLocalDate(endDate);
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate == null ? null : endDate.getTime();
    }

    // Tsurugiがis not nullを未サポートなので代わりにLocalDate.MAXを使う。
    public void setEndDate(LocalDate endDate) {
        this.endDate = endDate.equals(LocalDate.MAX)  ? null : DateUtils.toEpocMills(endDate);
    }


    public String getRule() {
        return rule;
    }

    public void setRule(String rule) {
        this.rule = rule;
    }

    public static Contract create(String phoneNumber, String startDate, String endDate, String rule) {
        Contract c = new Contract();
        c.phoneNumber = phoneNumber;
        c.startDate = DateUtils.toDate(startDate).getTime();
        c.endDate =  endDate == null ? null :  DateUtils.toDate(endDate).getTime();
        c.rule = rule;
        return c;
    }
}
