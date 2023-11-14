package com.tsurugidb.benchmark.phonebill.app.durability;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.db.entity.Billing;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.History;

public class DatabaseSnapshotTest {

    // ダミーのエンティティクラス
    private static class DummyEntity {
        private String phoneNumber;
        private String data;

        public DummyEntity(String phoneNumber, String data) {
            this.phoneNumber = phoneNumber;
            this.data = data;
        }

        public String getPhoneNumber() {
            return phoneNumber;
        }
    }

    // createMapFromList メソッドのテスト（正常ケース）
    @Test
    public void testCreateMapFromList() {
        List<DummyEntity> dummyEntities = Arrays.asList(
            new DummyEntity("000-0001", "Data1"),
            new DummyEntity("000-0002", "Data2")
        );

        Map<String, DummyEntity> result = DatabaseSnapshot.createMapFromList(
            dummyEntities, DummyEntity::getPhoneNumber);

        assertEquals(2, result.size(), "Map should contain two keys.");
        assertEquals("Data1", result.get("000-0001").data, "Data for phone number 000-0001 should match.");
        assertEquals("Data2", result.get("000-0002").data, "Data for phone number 000-0002 should match.");
    }

    // createMapFromList メソッドのテスト（重複キーの場合）
    @Test
    public void testCreateMapFromListWithDuplicateKeys() {
        List<DummyEntity> dummyEntitiesWithDuplicates = Arrays.asList(
            new DummyEntity("000-0001", "Data1"),
            new DummyEntity("000-0001", "Data3") // 重複する電話番号
        );

        assertThrows(RuntimeException.class, () -> {
            DatabaseSnapshot.createMapFromList(dummyEntitiesWithDuplicates, DummyEntity::getPhoneNumber);
        });
    }

    // mapPhoneNumbersToHistories メソッドのテスト（正常ケース）
    @Test
    public void testMapPhoneNumbersToHistories() {
        List<History> histories = Arrays.asList(
                History.create("001", "002", "C", "2022-03-05 12:10:01.999", 1, null, 0),
                History.create("002", "005", "R", "2022-03-05 12:10:11.999", 2, null, 0),
                History.create("003", "009", "R", "2022-03-06 12:10:01.999", 3, null, 0));

        Map<String, Set<History>> result = DatabaseSnapshot.mapPhoneNumbersToHistories(histories);

        assertEquals(3, result.size(), "Map should contain three keys.");
        assertTrue(result.containsKey("001"), "Map should contain key 001.");
        assertTrue(result.containsKey("005"), "Map should contain key 005.");
        assertTrue(result.containsKey("009"), "Map should contain key 009.");
    }

    // mapPhoneNumbersToHistories メソッドのテスト（無効な paymentCategory の場合）
    @Test
    public void testMapPhoneNumbersToHistoriesWithInvalidPaymentCategory() {
        List<History> historiesWithInvalidCategory = Arrays
                .asList(History.create("001", "002", "X", "2022-03-05 12:10:01.999", 1, null, 0));

        assertThrows(RuntimeException.class, () -> {
            DatabaseSnapshot.mapPhoneNumbersToHistories(historiesWithInvalidCategory);
        });
    }

    @Test
    public void testMapPhoneNumbersToHistoriesWithDuplicateHistory() {
        History duplicateHistory = History.create("001", "002", "C", "2022-03-05 12:10:01.999", 1, null, 0);
        List<History> historiesWithDuplicates = Arrays.asList(
            duplicateHistory,
            duplicateHistory.clone() // 同一の History オブジェクト
        );

        assertThrows(RuntimeException.class, () -> {
            DatabaseSnapshot.mapPhoneNumbersToHistories(historiesWithDuplicates);
        });
    }

    // コンストラクタとゲッターのテスト
    @Test
    public void testDatabaseSnapshotConstructorAndGetters() {
        List<History> testHistories = Arrays.asList(
            History.create("001", "002", "C", "2022-03-05 12:10:01.999", 1, null, 0),
            History.create("002", "005", "R", "2022-03-05 12:10:11.999", 2, null, 0)
        );
        List<Contract> testContracts = Arrays.asList(
            Contract.create("001", "2022-01-01", "2024-09-25", "dummy"),
            Contract.create("002", "2022-01-01", null, "dummy"),
            Contract.create("003", "2022-01-01", null, "dummy")
        );
        List<Billing> testBillings = Arrays.asList(
                Billing.create("001", "2022-01-01", 0, 0, 0, "id0"),
                Billing.create("002", "2022-01-01", 0, 0, 0, "id0"),
                Billing.create("003", "2022-01-01", 0, 0, 0, "id0"),
                Billing.create("004", "2022-01-01", 0, 0, 0, "id0")
            );

        DatabaseSnapshot snapshot = new DatabaseSnapshot(testHistories, testContracts, testBillings);

        // サイズを検証
        assertEquals(2, snapshot.getPhoneNumberToHistories().size());
        assertEquals(3, snapshot.getContracts().size());
        assertEquals(4, snapshot.getPhoneNumberToBilling().size());

        // 特定のキーに対する値を検証
        assertTrue(snapshot.getPhoneNumberToHistories().containsKey("001"));
        assertTrue(snapshot.getPhoneNumberToBilling().containsKey("001"));

        // Contractsは、入力と同じものがgetterで取得できる
        assertIterableEquals(testContracts, snapshot.getContracts());
    }
}
