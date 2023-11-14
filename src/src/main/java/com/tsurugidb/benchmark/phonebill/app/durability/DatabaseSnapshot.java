package com.tsurugidb.benchmark.phonebill.app.durability;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import com.tsurugidb.benchmark.phonebill.db.entity.Billing;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.History;

public class DatabaseSnapshot {
    private Map<String, Set<History>> phoneNumberToHistories;
    private List<Contract> contracts;
    private Map<String, Billing> phoneNumberToBilling;

    public DatabaseSnapshot(List<History> histories, List<Contract> contracts, List<Billing> billings) {
        this.phoneNumberToHistories = mapPhoneNumbersToHistories(histories);
        this.contracts = contracts;
        this.phoneNumberToBilling = createMapFromList(billings, Billing::getPhoneNumber);
    }

    static <T> Map<String, T> createMapFromList(List<T> list, Function<T, String> phoneNumberExtractor) {
        Map<String, T> map = new HashMap<>();
        for (T item : list) {
            String phoneNumber = phoneNumberExtractor.apply(item);
            if (map.containsKey(phoneNumber)) {
                throw new RuntimeException("Duplicate key found: " + phoneNumber);
            }
            map.put(phoneNumber, item);
        }
        return map;
    }

    public Map<String, Set<History>> getPhoneNumberToHistories() {
        return phoneNumberToHistories;
    }

    public List<Contract> getContracts() {
        return contracts;
    }

    public Map<String, Billing> getPhoneNumberToBilling() {
        return phoneNumberToBilling;
    }

    /**
     * Historyリストから電話番号をキーにしたHistoryオブジェクトのセットのマップを作成します。
     * 'C'のpaymentCategoryではcallerPhoneNumberを、'R'ではrecipientPhoneNumberをキーとします。
     * paymentCategoryが'C'または'R'以外の値の場合はRuntimeExceptionをスローします。
     *
     * @param histories Historyオブジェクトのリスト
     * @return 電話番号をキー、Historyオブジェクトのリストを値とするマップ
     * @throws RuntimeException paymentCategoryが'C'または'R'以外の場合
     */
    static Map<String, Set<History>> mapPhoneNumbersToHistories(List<History> histories) {
        Map<String, Set<History>> phoneNumberHistoryMap = new HashMap<>();

        for (History history : histories) {
            String keyPhoneNumber;

            switch (history.getPaymentCategorty()) {
                case "C":
                    keyPhoneNumber = history.getCallerPhoneNumber();
                    break;
                case "R":
                    keyPhoneNumber = history.getRecipientPhoneNumber();
                    break;
                default:
                    // paymentCategoryが'C'または'R'以外の場合、RuntimeExceptionをスロー
                    throw new RuntimeException("Invalid paymentCategory: " + history.getPaymentCategorty());
            }

            // 電話番号のキーがない場合はセットを作成し、Historyを追加
            Set<History> historySet = phoneNumberHistoryMap.computeIfAbsent(keyPhoneNumber, k -> new HashSet<>());
            if (!historySet.add(history)) {
                // すでにセットにHistoryが存在する場合は例外をスロー
                throw new RuntimeException("Duplicate history found for phone number: " + keyPhoneNumber);
            }
        }

        return phoneNumberHistoryMap;
    }
}

