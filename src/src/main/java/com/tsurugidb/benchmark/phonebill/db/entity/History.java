package com.tsurugidb.benchmark.phonebill.db.entity;

import java.io.Serializable;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import com.tsurugidb.benchmark.phonebill.util.DateUtils;

public class History implements Cloneable, Serializable{
    /**
     * SID サロゲートキーを使用しない場合は常に0が入る。
     */
    private long sid = 0;

    /**
     * 発信者電話番号
     */
    private String callerPhoneNumber;

    /**
     * 受信者電話番号
     */
    private String recipientPhoneNumber;

    /**
     * 料金区分(発信者負担(C)、受信社負担(R))
     */
    private String paymentCategorty;

    /**
     * 通話開始時刻
     */
    private long startTime;

    /**
     * 通話時間(秒)
     */
    private int timeSecs;

    /**
     * 料金
     */
    private Integer charge;

    /**
     * 削除フラグ
     */
    private int df = 0;

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("History [sid=");
        builder.append(sid);
        builder.append(", callerPhoneNumber=");
        builder.append(callerPhoneNumber);
        builder.append(", recipientPhoneNumber=");
        builder.append(recipientPhoneNumber);
        builder.append(", paymentCategorty=");
        builder.append(paymentCategorty);
        builder.append(", startTime=");
        builder.append(new Time(startTime));
        builder.append(", timeSecs=");
        builder.append(timeSecs);
        builder.append(", charge=");
        builder.append(charge);
        builder.append(", df=");
        builder.append(df);
        builder.append("]");
        return builder.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((callerPhoneNumber == null) ? 0 : callerPhoneNumber.hashCode());
        result = prime * result + ((charge == null) ? 0 : charge.hashCode());
        result = prime * result + df;
        result = prime * result + ((paymentCategorty == null) ? 0 : paymentCategorty.hashCode());
        result = prime * result + ((recipientPhoneNumber == null) ? 0 : recipientPhoneNumber.hashCode());
        result = prime * result + (int) (sid ^ (sid >>> 32));
        result = prime * result + (int) (startTime ^ (startTime >>> 32));
        result = prime * result + timeSecs;
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
        History other = (History) obj;
        if (callerPhoneNumber == null) {
            if (other.callerPhoneNumber != null)
                return false;
        } else if (!callerPhoneNumber.equals(other.callerPhoneNumber))
            return false;
        if (charge == null) {
            if (other.charge != null)
                return false;
        } else if (!charge.equals(other.charge))
            return false;
        if (df != other.df)
            return false;
        if (paymentCategorty == null) {
            if (other.paymentCategorty != null)
                return false;
        } else if (!paymentCategorty.equals(other.paymentCategorty))
            return false;
        if (recipientPhoneNumber == null) {
            if (other.recipientPhoneNumber != null)
                return false;
        } else if (!recipientPhoneNumber.equals(other.recipientPhoneNumber))
            return false;
        if (sid != other.sid)
            return false;
        if (startTime != other.startTime)
            return false;
        if (timeSecs != other.timeSecs)
            return false;
        return true;
    }

    @Override
    public History clone()  {
        try {
            return (History) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new InternalError(e);
        }
    }


    public Key getKey() {
        Key key = new Key();
        key.callerPhoneNumber = callerPhoneNumber;
        key.startTime = startTime;
        return key;
    }


    public String getCallerPhoneNumber() {
        return callerPhoneNumber;
    }

    public void setCallerPhoneNumber(String callerPhoneNumber) {
        this.callerPhoneNumber = callerPhoneNumber;
    }


    public String getRecipientPhoneNumber() {
        return recipientPhoneNumber;
    }

    public void setRecipientPhoneNumber(String recipientPhoneNumber) {
        this.recipientPhoneNumber = recipientPhoneNumber;
    }


    public String getPaymentCategorty() {
        return paymentCategorty;
    }

    public void setPaymentCategorty(String paymentCategorty) {
        this.paymentCategorty = paymentCategorty;
    }


    public Timestamp getStartTime() {
        return new Timestamp(startTime);
    }

    public LocalDateTime getStartTimeAsLocalDateTime() {
        return DateUtils.toLocalDateTime(startTime);
    }

    public void setStartTime(Timestamp startTime) {
        this.startTime = startTime.getTime();
    }

    public void setStartTime(LocalDateTime startTime) {
        this.startTime = DateUtils.toEpocMills(startTime);
    }

    public int getTimeSecs() {
        return timeSecs;
    }

    public void setTimeSecs(int timeSecs) {
        this.timeSecs = timeSecs;
    }


    public Integer getCharge() {
        return charge;
    }

    public void setCharge(Integer charge) {
        this.charge = charge;
    }


    public int getDf() {
        return df;
    }

    public void setDf(int df) {
        this.df = df;
    }

    public long getSid() {
        return sid;
    }

    public void setSid(long sid) {
        this.sid = sid;
    }

    /**
     * HistoryのPK
     */
    public static class Key {
        private String callerPhoneNumber;
        private long startTime;
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((callerPhoneNumber == null) ? 0 : callerPhoneNumber.hashCode());
            result = prime * result + (int) (startTime ^ (startTime >>> 32));
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
            if (callerPhoneNumber == null) {
                if (other.callerPhoneNumber != null)
                    return false;
            } else if (!callerPhoneNumber.equals(other.callerPhoneNumber))
                return false;
            if (startTime != other.startTime)
                return false;
            return true;
        }
        public String getCallerPhoneNumber() {
            return callerPhoneNumber;
        }
        public void setCallerPhoneNumber(String callerPhoneNumber) {
            this.callerPhoneNumber = callerPhoneNumber;
        }
        public Timestamp getStartTime() {
            return new Timestamp(startTime);
        }
        public void setStartTime(Timestamp startTime) {
            this.startTime = startTime.getTime();
        }
    }


    public static History create(String callerPhoneNumber,
            String recipientPhoneNumber,
            String paymentCategorty,
            String startTime,
            int timeSecs,
            Integer charge,
            int df) {
        History h = new History();
        h.callerPhoneNumber = callerPhoneNumber;
        h.recipientPhoneNumber = recipientPhoneNumber;
        h.paymentCategorty = paymentCategorty;
        h.startTime = DateUtils.toTimestamp(startTime).getTime();
        h.timeSecs = timeSecs;
        h.charge = charge;
        h.df = df;
        return h;
    }
}
