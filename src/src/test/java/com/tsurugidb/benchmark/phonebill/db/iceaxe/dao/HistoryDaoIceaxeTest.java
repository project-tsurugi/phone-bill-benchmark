package com.tsurugidb.benchmark.phonebill.db.iceaxe.dao;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Date;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.CreateTable;
import com.tsurugidb.benchmark.phonebill.app.billing.CalculationTarget;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.IceaxeTestTools;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;

class HistoryDaoIceaxeTest {
    private static final String ICEAXE_CONFIG_PATH = "src/test/config/iceaxe.properties";
    private static IceaxeTestTools testTools;
    private static PhoneBillDbManager manager;
    private static HistoryDaoIceaxe dao;
    private static DdlIceaxe ddl;

    @BeforeAll
    static void setUpBeforeClass() throws Exception {
        Config config = Config.getConfig(ICEAXE_CONFIG_PATH);
        new CreateTable().execute(config);
        testTools = new IceaxeTestTools(config);
        manager = testTools.getManager();
        dao = (HistoryDaoIceaxe) manager.getHistoryDao();
        ddl = (DdlIceaxe) manager.getDdl();
    }

    @AfterAll
    static void tearDownAfterClass() throws Exception {
        testTools.close();
    }

    @BeforeEach
    void setUp() throws Exception {
        testTools.execute(() -> ddl.dropTable("history"));
        testTools.execute(() -> ddl.dropTable("contracts"));
        testTools.execute(ddl::createHistoryTable);
        testTools.execute(ddl::createContractsTable);
    }

    @AfterEach
    void tearDown() throws Exception {
    }

    @Test
    final void testBatchInsert() {
        Set<History> expectedSet = new HashSet<>();
        List<History> list = new ArrayList<>();
        int[] ret;

        // 空のリストを渡したとき
        ret = testTools.execute(() -> {
            return dao.batchInsert(list);
        });
        assertEquals(0, ret.length);

        // 要素数 = 1のリスト
        History h = History.create("1", "99", "C", "2022-08-30 15:15:28.312", 5, 5, 1);
        expectedSet.add(h.clone());
        list.add(h.clone());

        ret = testTools.execute(() -> {
            return dao.batchInsert(list);
        });
        assertEquals(1, ret.length);
        assertEquals(expectedSet, testTools.getHistorySet());

        // 要素数 = 5のリスト
        list.clear();

        h.setCallerPhoneNumber("2");
        expectedSet.add(h.clone());
        list.add(h.clone());

        h.setCallerPhoneNumber("3");
        expectedSet.add(h.clone());
        list.add(h.clone());

        h.setCallerPhoneNumber("4");
        expectedSet.add(h.clone());
        list.add(h.clone());

        h.setCallerPhoneNumber("5");
        expectedSet.add(h.clone());
        list.add(h.clone());

        h.setCallerPhoneNumber("6");
        expectedSet.add(h.clone());
        list.add(h.clone());

        ret = testTools.execute(() -> {
            return dao.batchInsert(list);
        });
        assertEquals(5, ret.length);
        assertEquals(expectedSet, testTools.getHistorySet());
    }

    @Test
    final void testInsert() {
        Set<History> expectedSet = new HashSet<>();

        // 1件インサート
        History h = History.create("123", "456", "C", "2022-08-30 15:15:28.312", 5, 5, 1);
        expectedSet.add(h.clone());

        assertEquals(1, testTools.execute(() -> {
            return dao.insert(h);
        }));

        assertEquals(expectedSet, testTools.getHistorySet());

        // Null可なデータが全てNullのデータをインサート
        h.setCharge(null);
        h.setStartTime(DateUtils.toTimestamp("2022-08-30 16:15:28.512"));

        assertEquals(1, testTools.execute(() -> {
            return dao.insert(h);
        }));
        expectedSet.add(h.clone());

        assertEquals(expectedSet, testTools.getHistorySet());
    }

    @Test
    final void testGetMaxStartTime() {
        // 履歴が空のとき

        assertEquals(DateUtils.toEpocMills(LocalDate.MIN), testTools.execute(() -> {
            return dao.getMaxStartTime();
        }));

        // 履歴が複数存在するとき
        testTools.insertToHistory("Phone-0001", "Phone-0008", "C", "2020-10-31 23:59:59.999", 30, null, 0);
        testTools.insertToHistory("Phone-0001", "Phone-0008", "C", "2020-11-01 00:00:00.000", 30, null, 0);
        testTools.insertToHistory("Phone-0001", "Phone-0008", "C", "2021-11-15 12:12:12.000", 90, null, 1);
        testTools.insertToHistory("Phone-0001", "Phone-0008", "C", "2020-11-30 23:59:59.999", 90, null, 0);
        testTools.insertToHistory("Phone-0001", "Phone-0008", "C", "2020-12-01 00:00:00.000", 30, null, 0);
        testTools.insertToHistory("Phone-0005", "Phone-0001", "C", "2020-11-10 00:00:00.000", 25, null, 0);
        testTools.insertToHistory("Phone-0005", "Phone-0008", "R", "2020-11-30 00:00:00.000", 30, null, 0);

        assertEquals(DateUtils.toTimestamp("2021-11-15 12:12:12.000").getTime(), testTools.execute(() -> {
            return dao.getMaxStartTime();
        }));

    }

    @Test
    final void testUpdate() {
        History h1 = History.create("001", "456", "C", "2022-01-10 15:15:28.312", 5, 2, 0);
        History h2 = History.create("001", "456", "C", "2022-01-10 15:15:28.313", 5, null, 0);
        History h3 = History.create("001", "456", "C", "2022-01-10 15:15:28.314", 5, 2, 1);
        History h4 = History.create("001", "456", "C", "2022-01-05 00:00:00.000", 5, 2, 0);
        History h5 = History.create("001", "459", "C", "2022-01-10 15:15:28.312", 3, null, 1);

        // 空のテーブルに対してアップデート

        assertEquals(0, testTools.execute(() -> {
            return dao.update(h1);
        }));

        // テストデータを入れる
        Set<History> testDataSet = new HashSet<>(Arrays.asList(h1, h2, h3));
        testTools.insertToHistory(testDataSet);
        assertEquals(testDataSet, testTools.getHistorySet());

        // 更新対象のレコードがないケース
        assertEquals(0, testTools.execute(() -> {
            return dao.update(h4);
        }));
        assertEquals(testDataSet, testTools.getHistorySet());

        // 同じ値で更新
        assertEquals(1, testTools.execute(() -> {
            return dao.update(h1);
        }));
        assertEquals(testDataSet, testTools.getHistorySet());

        // キー値以外を全て更新
        assertEquals(1, testTools.execute(() -> {
            return dao.update(h5);
        }));
        testDataSet.add(h5);
        testDataSet.remove(h1);
        assertEquals(testDataSet, testTools.getHistorySet());

    }

    @Test
    final void testBatchUpdate() {
        History h1 = History.create("001", "456", "C", "2022-01-10 15:15:28.312", 5, 2, 0);
        History h2 = History.create("001", "456", "C", "2022-01-10 15:15:28.313", 5, null, 0);
        History h3 = History.create("001", "456", "C", "2022-01-10 15:15:28.314", 5, 2, 1);

        History h2u = History.create("001", "456", "C", "2022-01-10 15:15:28.313", 15, 5, 1);
        History h3u = History.create("001", "456", "C", "2022-01-10 15:15:28.314", 15, 30, 0);
        History h4u = History.create("001", "456", "C", "2022-01-05 00:00:00.000", 5, 2, 0); // 同一キーのデータがないのでアップデートされない

        // テストデータを入れる
        Set<History> testDataSet = new HashSet<>(Arrays.asList(h1, h2, h3));
        testTools.insertToHistory(testDataSet);;
        assertEquals(testDataSet, testTools.getHistorySet());

        // アップデート実行
        List<History> updateDataList = Arrays.asList(h2u, h4u, h3u);
        assertEquals(3, testTools.execute(() -> {
            return dao.batchUpdate(updateDataList);
        }));
        testDataSet.remove(h2);
        testDataSet.remove(h3);
        testDataSet.add(h2u);
        testDataSet.add(h3u);
        assertEquals(testDataSet, testTools.getHistorySet());


    }

    @Test
    final void testGetHistoriesKey() {
        // 契約
        Contract c1 = Contract.create("001", "2022-01-01", "2024-09-25", "dummy");
        Contract c2 = Contract.create("002", "2022-01-01", null, "dummy");
        Contract c3 = Contract.create("899", "2022-01-01", null, "dummy");
        Contract c4 = Contract.create("999", "2022-01-01", null, "dummy");

        testTools.insertToContracts(c1, c2, c3);

        // 契約c1の履歴データ
        History h11 = History.create("001", "002", "C", "2022-03-05 12:10:01.999", 11, null, 0);
        History h12 = History.create("001", "005", "C", "2022-03-05 12:10:11.999", 12, null, 0);
        History h13 = History.create("001", "009", "C", "2022-03-06 12:10:01.999", 13, null, 0);
        History h14 = History.create("001", "002", "C", "2021-12-31 23:59:59.999", 14, null, 0); // 境界値
        History h15 = History.create("001", "005", "C", "2022-01-01 00:00:00.000", 15, null, 0); // 境界値
        History h16 = History.create("001", "005", "C", "2024-09-25 23:59:59.999", 16, null, 0); // 境界値
        History h17 = History.create("001", "005", "C", "2024-09-26 00:00:00.000", 17, null, 0); // 境界値
        testTools.insertToHistory(h11, h12, h13, h14, h15, h16, h17);

        // 契約c2の履歴データ
        History h21 = History.create("002", "005", "C", "2022-03-05 12:10:01.999", 21, null, 0);
        History h22 = History.create("002", "001", "C", "2022-05-05 12:10:01.999", 22, null, 0);
        History h23 = History.create("002", "007", "C", "2022-09-15 12:10:01.999", 23, null, 0);
        History h24 = History.create("002", "008", "C", "2021-12-31 23:59:59.999", 14, null, 0); // 境界値
        History h25 = History.create("002", "009", "C", "2022-01-01 00:00:00.000", 15, null, 0); // 境界値
        testTools.insertToHistory(h21, h22, h23, h24, h25);

        // 契約c1, c2以外の履歴データ
        History h7 = History.create("003", "005", "C", "2022-06-05 12:10:01.999", 7, null, 0);
        History h8 = History.create("004", "001", "C", "2022-01-05 12:10:01.999", 8, null, 0);
        History h9 = History.create("005", "007", "C", "2022-09-18 12:10:01.999", 9, null, 0);
        testTools.insertToHistory(h7, h8, h9);


        Set<History> expectedSet = new HashSet<>();
        Set<History> actualSet = new HashSet<>();

        // c1: 契約終了日が存在する契約
        expectedSet.clear();
        expectedSet.addAll(Arrays.asList(h11, h12, h13, h15, h16));
        actualSet.clear();
        actualSet.addAll(testTools.execute(() -> {
            return dao.getHistories(c1.getKey());
        }));

        assertEquals(expectedSet, actualSet);

        // c2 契約終了日が存在しない契約
        expectedSet.clear();
        expectedSet.addAll(Arrays.asList(h21, h22, h23, h25));
        actualSet.clear();
        actualSet.addAll(testTools.execute(() -> {
            return dao.getHistories(c2.getKey());
        }));
        assertEquals(expectedSet, actualSet);

        // c3: 履歴を持たない契約を指定したケース
        assertEquals(Collections.EMPTY_LIST, testTools.execute(() -> {
            return dao.getHistories(c3.getKey());
        }));

        // c4: 履歴テーブルに存在しない契約を指定したケース
        assertEquals(Collections.EMPTY_LIST, testTools.execute(() -> {
            return dao.getHistories(c4.getKey());
        }));

    }

    @Test
    final void testGetHistoriesCalculationTarget() {
        Contract c = Contract.create("001", "2022-01-01", "2024-09-25", "dummy");
        Date start = DateUtils.toDate("2022-01-05");
        Date end = DateUtils.toDate("2022-02-03");
        CalculationTarget target = new CalculationTarget(c, null, null, start, end, false);

        // 履歴が空の時
        assertEquals(Collections.emptyList(), testTools.execute(() -> {
            return dao.getHistories(target);
        }));

        // テスト用の履歴データ => 通話料金が発信者負担
        History h001 = History.create("001", "456", "C", "2022-01-10 15:15:28.312", 5, 2, 0); // 検索対象
        History h002 = History.create("001", "456", "C", "2022-01-10 15:15:28.313", 5, null, 0); // 検索対象 -> chargeがnull
        History h003 = History.create("001", "456", "C", "2022-01-10 15:15:28.314", 5, 2, 1); // 検索対象外 -> 論理削除フラグ
        History h004 = History.create("001", "456", "C", "2022-01-05 00:00:00.000", 5, 2, 0); // 検索対象 -> startの境界値
        History h005 = History.create("001", "456", "C", "2022-01-05 00:00:00.001", 5, 2, 0); // 検索対象 -> startの境界値
        History h006 = History.create("001", "456", "C", "2022-01-04 23:59:59.999", 5, 2, 0); // 検索対象外 -> startの境界値
        History h007 = History.create("001", "456", "C", "2022-02-04 00:00:00.000", 5, 2, 0); // 検索対象外 -> endの境界値
        History h008 = History.create("001", "456", "C", "2022-02-04 00:00:00.001", 5, 2, 0); // 検索対象外 -> endの境界値
        History h009 = History.create("001", "456", "C", "2022-02-03 23:59:59.999", 5, 2, 0); // 検索対象 -> endの境界値
        History h010 = History.create("002", "456", "C", "2022-01-10 15:15:28.312", 5, 2, 0); // 検索対象外 -> 電話番号が違う

        // テスト用の履歴データ => 通話料金が受信者負担
        History h101 = History.create("151", "001", "R", "2022-01-10 15:15:28.315", 5, 2, 0); // 検索対象
        History h102 = History.create("151", "001", "R", "2022-01-10 15:15:28.316", 5, null, 0); // 検索対象 -> chargeがnull
        History h103 = History.create("151", "001", "R", "2022-01-10 15:15:28.317", 5, 2, 1); // 検索対象外 -> 論理削除フラグ
        History h104 = History.create("151", "001", "R", "2022-01-05 00:00:00.000", 5, 2, 0); // 検索対象 -> startの境界値
        History h105 = History.create("151", "001", "R", "2022-01-05 00:00:00.001", 5, 2, 0); // 検索対象 -> startの境界値
        History h106 = History.create("151", "001", "R", "2022-01-04 23:59:59.999", 5, 2, 0); // 検索対象外 -> startの境界値
        History h107 = History.create("151", "001", "R", "2022-02-04 00:00:00.000", 5, 2, 0); // 検索対象外 -> endの境界値
        History h108 = History.create("151", "001", "R", "2022-02-04 00:00:00.001", 5, 2, 0); // 検索対象外 -> endの境界値
        History h109 = History.create("151", "001", "R", "2022-02-03 23:59:59.999", 5, 2, 0); // 検索対象 -> endの境界値
        History h110 = History.create("151", "002", "R", "2022-01-10 15:15:28.312", 5, 2, 0); // 検索対象外 -> 電話番号が違う

        // テストデータを投入
        Set<History> histories = new HashSet<>();
        histories.add(h001);
        histories.add(h002);
        histories.add(h003);
        histories.add(h004);
        histories.add(h005);
        histories.add(h006);
        histories.add(h007);
        histories.add(h008);
        histories.add(h009);
        histories.add(h010);
        histories.add(h101);
        histories.add(h102);
        histories.add(h103);
        histories.add(h104);
        histories.add(h105);
        histories.add(h106);
        histories.add(h107);
        histories.add(h108);
        histories.add(h109);
        histories.add(h110);

        testTools.insertToHistory(histories);

        assertEquals(histories , testTools.getHistorySet());

        // 期待通りのレコードがselectされることを確認
        HashSet<History> actual = testTools.execute(() -> {
            return new HashSet<>(dao.getHistories(target));
        });

        assertTrue(actual.contains(h001));
        assertTrue(actual.contains(h002));
        assertFalse(actual.contains(h003));
        assertTrue(actual.contains(h004));
        assertTrue(actual.contains(h005));
        assertFalse(actual.contains(h006));
        assertFalse(actual.contains(h007));
        assertFalse(actual.contains(h008));
        assertTrue(actual.contains(h009));
        assertFalse(actual.contains(h010));

        assertTrue(actual.contains(h101));
        assertTrue(actual.contains(h102));
        assertFalse(actual.contains(h103));
        assertTrue(actual.contains(h104));
        assertTrue(actual.contains(h105));
        assertFalse(actual.contains(h106));
        assertFalse(actual.contains(h107));
        assertFalse(actual.contains(h108));
        assertTrue(actual.contains(h109));
        assertFalse(actual.contains(h110));

    }


    @Test
    final void testGetHistories() {
        Set<History> actualSet;

        // テーブルが空の時
        assertEquals(Collections.EMPTY_LIST, testTools.execute(() -> {
            return dao.getHistories();
        }));

        // テーブルに5レコード追加
        Set<History> set = new HashSet<>();
        set.add(History.create("1", "456", "C", "2022-08-30 15:15:28.312", 5, 5, 1));
        set.add(History.create("2", "456", "C", "2022-08-30 15:15:28.312", 5, 5, 1));
        set.add(History.create("3", "456", "C", "2022-08-30 15:15:28.312", 5, null, 1));
        set.add(History.create("4", "456", "C", "2022-08-30 15:15:28.312", 5, 5, 1));
        set.add(History.create("5", "456", "C", "2022-08-30 15:15:28.312", 5, null, 1));
        testTools.insertToHistory(set);

        actualSet = testTools.execute(() -> {
            return dao.getHistories();
        }).stream().collect(Collectors.toSet());
        assertEquals(set, actualSet);
    }

    @Test
    final void testUpdateChargeNull() {
        Set<History> actualSet;

        // テーブルに5レコード追加
        Set<History> set = new HashSet<>();
        set.add(History.create("1", "456", "C", "2022-08-30 15:15:28.312", 5, 5, 1));
        set.add(History.create("2", "456", "C", "2022-08-30 15:15:28.312", 5, 5, 1));
        set.add(History.create("3", "456", "C", "2022-08-30 15:15:28.312", 5, null, 1));
        set.add(History.create("4", "456", "C", "2022-08-30 15:15:28.312", 5, 5, 1));
        set.add(History.create("5", "456", "C", "2022-08-30 15:15:28.312", 5, null, 1));
        testTools.insertToHistory(set);

        // インサートされた値の確認
        actualSet = testTools.execute(() -> {
            return dao.getHistories();
        }).stream().collect(Collectors.toSet());
        assertEquals(set, actualSet);

        // updateChargeNull
        int ret = testTools.execute(dao::updateChargeNull);
        assertEquals(5, ret);

        // chargeの値がすべてNULLになっていることを確認
        set.stream().forEach(h -> h.setCharge(null));
        set = new HashSet<>(set);
        actualSet = testTools.execute(() -> {
            return dao.getHistories();
        }).stream().collect(Collectors.toSet());
        assertEquals(set, actualSet);
    }

    @Test
    final void testDelete() {
        Set<History> actualSet;

        // テーブルに5レコード追加
        History h1 = History.create("001", "456", "C", "2022-01-10 15:15:28.312", 5, 2, 0);
        History h21 = History.create("002", "456", "C", "2022-01-10 15:15:28.313", 5, null, 0);
        History h22 = History.create("002", "456", "C", "2022-01-10 15:15:28.314", 5, 2, 1);
        History h3 = History.create("003", "456", "C", "2022-01-05 00:00:00.000", 5, 2, 0);
        History h4 = History.create("004", "459", "C", "2022-01-10 15:15:28.312", 3, null, 1);

        Set<History> set = new HashSet<>(Arrays.asList(h1, h21, h22,  h3, h4));
        testTools.execute(() -> {
            dao.batchInsert(set);
        });

        // インサートされた値の確認
        actualSet = testTools.execute(() -> {
            return dao.getHistories();
        }).stream().collect(Collectors.toSet());
        assertEquals(set, actualSet);

        // 1件delete
        assertEquals(1, testTools.execute(() -> {
            return dao.delete("003");
        }));
        set.remove(h3);
        actualSet = testTools.execute(() -> {
            return dao.getHistories();
        }).stream().collect(Collectors.toSet());
        assertEquals(set, actualSet);

        // 複数件delete
        assertEquals(2, testTools.execute(() -> {
            return dao.delete("002");
        }));
        set.removeAll(Arrays.asList(h21, h22));
        actualSet = testTools.execute(() -> {
            return dao.getHistories();
        }).stream().collect(Collectors.toSet());
        assertEquals(set, actualSet);
    }

    @Test
    final void testGetAllPhoneNumbers() {
        Set<History> actualSet;

        // テーブルに5レコード追加
        History h1 = History.create("001", "456", "C", "2022-01-10 15:15:28.312", 5, 2, 0);
        History h21 = History.create("002", "456", "C", "2022-01-10 15:15:28.313", 5, null, 0);
        History h22 = History.create("002", "456", "C", "2022-01-10 15:15:28.314", 5, 2, 1);
        History h3 = History.create("003", "456", "C", "2022-01-05 00:00:00.000", 5, 2, 0);
        History h4 = History.create("004", "459", "C", "2022-01-10 15:15:28.312", 3, null, 1);

        Set<History> set = new HashSet<>(Arrays.asList(h1, h21, h22,  h3, h4));
        testTools.execute(() -> {
            dao.batchInsert(set);
        });

        // インサートされた値の確認
        actualSet = testTools.execute(() -> {
            return dao.getHistories();
        }).stream().collect(Collectors.toSet());
        assertEquals(set, actualSet);

        // テスト実行
        Set<String> expect = set.stream().map(h -> h.getCallerPhoneNumber()).collect(Collectors.toSet());
        assertEquals(expect, testTools.execute(() -> {
            return dao.getAllPhoneNumbers();
        }).stream().collect(Collectors.toSet()));
    }

    @Test
    final void testCount() {
        // 履歴が空のとき

        assertEquals(0l, testTools.execute(dao::count));

        // 履歴が複数存在するとき
        testTools.insertToHistory("Phone-0001", "Phone-0008", "C", "2020-10-31 23:59:59.999", 30, null, 0);
        testTools.insertToHistory("Phone-0001", "Phone-0008", "C", "2020-11-01 00:00:00.000", 30, null, 0);
        testTools.insertToHistory("Phone-0001", "Phone-0008", "C", "2021-11-15 12:12:12.000", 90, null, 1);
        assertEquals(3l, testTools.execute(dao::count));

        testTools.insertToHistory("Phone-0001", "Phone-0008", "C", "2020-11-30 23:59:59.999", 90, null, 0);
        testTools.insertToHistory("Phone-0001", "Phone-0008", "C", "2020-12-01 00:00:00.000", 30, null, 0);
        testTools.insertToHistory("Phone-0005", "Phone-0001", "C", "2020-11-10 00:00:00.000", 25, null, 0);
        testTools.insertToHistory("Phone-0005", "Phone-0008", "R", "2020-11-30 00:00:00.000", 30, null, 0);
        assertEquals(7l, testTools.execute(dao::count));
    }

}
