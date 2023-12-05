package com.tsurugidb.benchmark.phonebill.app.durability;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.app.durability.ConfirmDbIntegrity.LogType;
import com.tsurugidb.benchmark.phonebill.app.durability.ConfirmDbIntegrity.TransactionStatus;

class ConfirmDbIntegrityTest {


    @Test
    public void testCompareAndReportWithNonMatchingCollections() {
        // テストデータのセットアップ
        DummyEntity entity10 = new DummyEntity("Key1", "Value0");
        DummyEntity entity20 = new DummyEntity("Key2", "Value0");
        DummyEntity entity21 = new DummyEntity("Key2", "Value1");
        DummyEntity entity30 = new DummyEntity("Key3", "Value0");

        List<DummyEntity> actualList = Arrays.asList(entity10, entity20);
        List<DummyEntity> expectedList1 = Arrays.asList(entity10, entity20);
        List<DummyEntity> expectedList2 = Arrays.asList(entity10, entity30);
        List<DummyEntity> expectedList3 = Arrays.asList(entity10, entity21);

        ConfirmDbIntegrity confirmDbIntegrity = new ConfirmDbIntegrity();
        confirmDbIntegrity.out= new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8), true);

        // メソッドの実行
        confirmDbIntegrity.exactlyEqual =true;
        confirmDbIntegrity.compareAndReport("DummyEntity", DummyEntity::getKey,
                Object::equals, actualList, expectedList1);
        assertTrue(confirmDbIntegrity.exactlyEqual);


        confirmDbIntegrity.exactlyEqual =true;
        confirmDbIntegrity.compareAndReport("DummyEntity", DummyEntity::getKey,
                Object::equals, actualList, expectedList2);
        assertFalse(confirmDbIntegrity.exactlyEqual);


        confirmDbIntegrity.exactlyEqual =true;
        confirmDbIntegrity.compareAndReport("DummyEntity", DummyEntity::getKey,
                Object::equals, actualList, expectedList3);
        assertFalse(confirmDbIntegrity.exactlyEqual);

        confirmDbIntegrity.exactlyEqual =true;
        confirmDbIntegrity.compareAndReport("DummyEntity", DummyEntity::getKey,
                Object::equals, actualList, Arrays.asList(expectedList1, expectedList2, expectedList3));
        assertTrue(confirmDbIntegrity.exactlyEqual);

        confirmDbIntegrity.exactlyEqual =true;
        confirmDbIntegrity.compareAndReport("DummyEntity", DummyEntity::getKey,
                Object::equals, actualList, Arrays.asList(expectedList3, expectedList2, expectedList1));
        assertTrue(confirmDbIntegrity.exactlyEqual);

        confirmDbIntegrity.exactlyEqual =true;
        confirmDbIntegrity.compareAndReport("DummyEntity", DummyEntity::getKey,
                Object::equals, actualList, Arrays.asList(expectedList2, expectedList3));
        assertFalse(confirmDbIntegrity.exactlyEqual);

    }

    @Test
    public void testGetAllTransactionIdKeyMaps() {
        String logData = "17:52:18.591 [pool-1-thread-10] DEBUG c.t.b.p.a.b.CalculationTask-cmt - Transaction committing, tid = TID-0000000000000039, txOption = LTX{label=BATCH_MAIN, writePreserve=[history, billing]}, key = 00000000859\n"
                + "17:52:19.418 [pool-1-thread-7] DEBUG c.t.b.p.app.billing.CalculationTask - TIME INFO: tid = TID-0000000600000025, exec time = 52354, commit to abort time = 105384\n"
                + "17:52:18.707 [pool-1-thread-10] DEBUG c.t.b.p.a.b.CalculationTask-cmt - Transaction completed, tid = TID-0000000000000039, txOption = LTX{label=BATCH_MAIN, writePreserve=[history, billing]}, key = 00000000859, update/insert records = 238\n"
                + "17:52:14.962 [pool-1-thread-11] DEBUG c.t.b.p.a.b.CalculationTask-cmt - Transaction committing, tid = TID-0000000000000022, txOption = LTX{label=BATCH_MAIN, writePreserve=[history, billing]}, key = 00000000193\n"
                + "17:52:15.071 [pool-1-thread-11] DEBUG c.t.b.p.a.b.CalculationTask-cmt - Transaction aborted, tid = TID-0000000000000022, txOption = LTX{label=BATCH_MAIN, writePreserve=[history, billing]}, key = 00000000193, exception = dummy\n"
                // ↓不完全な行 => 無視される
                + "17:52:15.075 [pool-1-thread-11] DEBUG c.t.b.p.a.b.CalculationTask-cmt - Transaction aborted, tid = TID-0000000000000023, txOption = LTX{label=BATCH_MAIN, writePreserve=[history, billing]}, key = 0000000019";

        ByteArrayInputStream inputStream = new ByteArrayInputStream(logData.getBytes(StandardCharsets.UTF_8));
        ConfirmDbIntegrity confirmDbIntegrity = new ConfirmDbIntegrity();
        Map<LogType, Map<String, String>> result = confirmDbIntegrity.getAllTransactionIdKeyMaps(inputStream);

        // 基本的な検証
        assertEquals(2, result.get(LogType.COMMITTING).size());
        assertEquals(1, result.get(LogType.COMPLETED).size());
        assertEquals(1, result.get(LogType.ABORTED).size());

        // より詳細な検証
        assertEquals("00000000859", result.get(LogType.COMMITTING).get("TID-0000000000000039"));
        assertEquals("00000000859", result.get(LogType.COMPLETED).get("TID-0000000000000039"));
        assertEquals("00000000193", result.get(LogType.COMMITTING).get("TID-0000000000000022"));
        assertEquals("00000000193", result.get(LogType.ABORTED).get("TID-0000000000000022"));
    }

    @Test
    public void testGetAllTransactionIdKeyMapsThrowsExceptionOnDuplicateTid() {
        String logDataWithDuplicateTid =
            "17:52:18.591 [pool-1-thread-10] DEBUG c.t.b.p.a.b.CalculationTask-cmt - Transaction committing, tid = TID-0000000000000039, txOption = LTX{label=BATCH_MAIN, writePreserve=[history, billing]}, key = 00000000859\n" +
            "17:52:18.707 [pool-1-thread-10] DEBUG c.t.b.p.a.b.CalculationTask-cmt - Transaction completed, tid = TID-0000000000000039, txOption = LTX{label=BATCH_MAIN, writePreserve=[history, billing]}, key = 00000000859, update/insert records = 238\n" +
            "17:52:18.707 [pool-1-thread-10] DEBUG c.t.b.p.a.b.CalculationTask-cmt - Transaction completed, tid = TID-0000000000000039, txOption = LTX{label=BATCH_MAIN, writePreserve=[history, billing]}, key = 00000000859, update/insert records = 238\n" +
            "17:52:15.071 [pool-1-thread-11] DEBUG c.t.b.p.a.b.CalculationTask-cmt - Transaction aborted, tid = TID-0000000000000039, txOption = LTX{label=BATCH_MAIN, writePreserve=[history, billing]}, key = 00000000193, exception = dummy\n";

        ByteArrayInputStream inputStream = new ByteArrayInputStream(logDataWithDuplicateTid.getBytes(StandardCharsets.UTF_8));
        ConfirmDbIntegrity confirmDbIntegrity = new ConfirmDbIntegrity();

        // 重複するTIDがある場合にRuntimeExceptionがスローされることを確認
        assertThrows(RuntimeException.class, () -> {
            confirmDbIntegrity.getAllTransactionIdKeyMaps(inputStream);
        });
    }

    @Test
    public void testDetermineTransactionStatus() {
        ConfirmDbIntegrity dbIntegrity = new ConfirmDbIntegrity();

        // テストデータの準備
        Set<String> phoneNumbers = new HashSet<>();
        phoneNumbers.add("000-0001");
        phoneNumbers.add("000-0002");
        phoneNumbers.add("000-0003");
        phoneNumbers.add("000-0004");


        Map<String, String> abortedTransactions = new HashMap<>();
        abortedTransactions.put("TID-01", "000-0001");

        Map<String, String> committingTransactions = new HashMap<>();
        committingTransactions.put("TID-01", "000-0001");
        committingTransactions.put("TID-02", "000-0002");
        committingTransactions.put("TID-03", "000-0003");


        Map<String, String> completedTransactions = new HashMap<>();
        completedTransactions.put("TID-03", "000-0003");

        // トランザクション状態の決定
        Map<String, TransactionStatus> transactionStatuses = dbIntegrity.determineTransactionStatus(
                phoneNumbers, abortedTransactions, committingTransactions, completedTransactions);

        // 期待される結果を検証
        assertEquals(TransactionStatus.UNCOMMITTED, transactionStatuses.get("000-0001"));
        assertEquals(TransactionStatus.COMMITTING, transactionStatuses.get("000-0002"));
        assertEquals(TransactionStatus.COMMITTED, transactionStatuses.get("000-0003"));
        assertEquals(TransactionStatus.UNCOMMITTED, transactionStatuses.get("000-0004"));


        // 開始していないTXがAbortしていた場合
        abortedTransactions.put("TID-04", "000-0004");
        assertThrows(RuntimeException.class, () -> {
            dbIntegrity.determineTransactionStatus(phoneNumbers, abortedTransactions, committingTransactions,
                    completedTransactions);
        });
    }


    @Test
    public void testCalculatePhoneNumberOccurrences() {
        // テスト用のトランザクションデータを準備
        Map<String, String> transactionMap = new HashMap<>();
        transactionMap.put("TID-01", "000-0001");
        transactionMap.put("TID-02", "000-0002");
        transactionMap.put("TID-03", "000-0001"); // 重複する電話番号

        ConfirmDbIntegrity confirmDbIntegrity = new ConfirmDbIntegrity();

        // 出現回数を計算
        Map<String, Integer> occurrences = confirmDbIntegrity.calculatePhoneNumberOccurrences(transactionMap);

        // 期待される結果を検証
        assertEquals(2, occurrences.get("000-0001")); // 電話番号 "000-0001" は2回出現
        assertEquals(1, occurrences.get("000-0002")); // 電話番号 "000-0002" は1回出現
        assertNull(occurrences.get("000-0003")); // 存在しない電話番号
        assertEquals(2, occurrences.size());
    }



    @Test
    public void testCalculatePhoneNumberOccurrencesWithEmptyMap() {
        // 空のトランザクションデータ
        Map<String, String> emptyTransactionMap = new HashMap<>();

        ConfirmDbIntegrity confirmDbIntegrity = new ConfirmDbIntegrity();

        // 出現回数を計算
        Map<String, Integer> occurrences = confirmDbIntegrity.calculatePhoneNumberOccurrences(emptyTransactionMap);

        // 結果のマップが空であることを確認
        assertTrue(occurrences.isEmpty(), "The occurrences map should be empty for an empty input map.");

        // エントリ数が0であることを確認
        assertEquals(0, occurrences.size(), "The size of the occurrences map should be zero for an empty input map.");
    }


    public static class DummyEntity {
        private String key;
        private String value;

        public DummyEntity(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            DummyEntity other = (DummyEntity) obj;
            if (key == null) {
                if (other.key != null)
                    return false;
            } else if (!key.equals(other.key))
                return false;
            if (value == null) {
                if (other.value != null)
                    return false;
            } else if (!value.equals(other.value))
                return false;
            return true;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((key == null) ? 0 : key.hashCode());
            result = prime * result + ((value == null) ? 0 : value.hashCode());
            return result;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("DummyEntity [key=");
            builder.append(key);
            builder.append(", value=");
            builder.append(value);
            builder.append("]");
            return builder.toString();
        }
    }

}

