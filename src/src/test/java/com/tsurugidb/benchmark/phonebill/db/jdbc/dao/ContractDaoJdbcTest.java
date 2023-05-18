package com.tsurugidb.benchmark.phonebill.db.jdbc.dao;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Date;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.AbstractJdbcTestCase;
import com.tsurugidb.benchmark.phonebill.db.dao.ContractDao;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract.Key;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;

class ContractDaoJdbcTest extends AbstractJdbcTestCase {
    private static final Contract C10 = Contract.create("1", "2022-01-01", "2022-03-12", "dummy");
    private static final Contract C20 = Contract.create("2", "2022-02-01", null, "dummy2");
    private static final Contract C30 = Contract.create("3", "2020-01-01", "2021-03-12", "dummy3");
    private static final Contract C31 = Contract.create("3", "2022-04-01", "2022-09-12", "dummy4");
    private static final Contract C40 = Contract.create("4", "2000-01-01", "2001-03-12", "dummy5");
    private static final Contract C41 = Contract.create("4", "2002-04-01", null, "dummy6");



    @Test
    void testBatchInsert() throws SQLException {
        ContractDao dao = getManager().getContractDao();
        testBatchInsertSub(dao);
    }

    protected void testBatchInsertSub(ContractDao dao) throws SQLException {
        truncateTable("contracts");

        Set<Contract> expectedSet = new HashSet<>();
        List<Contract> list = new ArrayList<>();
        int[] ret;

        // 空のリストを渡したとき
        ret = dao.batchInsert(list);
        assertEquals(0, ret.length);

        // 要素数 = 1のリスト
        Contract c = Contract.create("1", "2022-01-01", "2022-12-12", "dummy");
        expectedSet.add(c.clone());
        list.add(c.clone());

        ret = dao.batchInsert(list);
        assertEquals(1, ret.length);
        assertEquals(expectedSet, getContractSet());

        // 要素数 = 5のリスト
        list.clear();

        c.setPhoneNumber("2");
        expectedSet.add(c.clone());
        list.add(c.clone());

        c.setPhoneNumber("3");
        expectedSet.add(c.clone());
        list.add(c.clone());

        c.setPhoneNumber("4");
        expectedSet.add(c.clone());
        list.add(c.clone());

        c.setPhoneNumber("5");
        expectedSet.add(c.clone());
        list.add(c.clone());

        c.setPhoneNumber("6");
        expectedSet.add(c.clone());
        list.add(c.clone());

        ret =  dao.batchInsert(list);
        assertEquals(5, ret.length);
        assertEquals(expectedSet, getContractSet());
    }

    @Test
    final void testInsert() throws SQLException {
        ContractDao dao = getManager().getContractDao();
        truncateTable("contracts");

        Set<Contract> expectedSet = new HashSet<>();

        // 1件インサート
        Contract c = Contract.create("1", "2022-01-01", "2022-12-12", "dummy");
        expectedSet.add(c.clone());
        dao.insert(c);
        assertEquals(expectedSet, getContractSet());

        // Null可なデータが全てNullのデータをインサート
        c.setPhoneNumber("2");
        c.setEndDate((Date) null);
        expectedSet.add(c.clone());
        dao.insert(c);
        assertEquals(expectedSet, getContractSet());
    }

    @Test
    final void testUpdate() throws SQLException {
        ContractDao dao = getManager().getContractDao();
        truncateTable("contracts");

        Contract c1 = C10;
        Contract c2 = Contract.create("2", "2062-01-01", null, "dummy");
        Contract c3 = Contract.create("3", "2020-01-01", "2021-12-12", "dummy");
        Contract c4 = Contract.create("3", "2022-01-01", null, "dummy");
        Contract c5 = Contract.create("1", "2022-01-01", "2023-03-02", "dummyA");

        // 空のテーブルに対してアップデート

        assertEquals(0, dao.update(c1));

        // テストデータを入れる
        Set<Contract> testDataSet = new HashSet<>(Arrays.asList(c1, c2, c3));
        dao.batchInsert(testDataSet);
        assertEquals(testDataSet, getContractSet());

        // 更新対象のレコードがないケース
        assertEquals(0, dao.update(c4));
        assertEquals(testDataSet, getContractSet());

        // 同じ値で更新
        assertEquals(1, dao.update(c1));
        assertEquals(testDataSet, getContractSet());

        // キー値以外を全て更新
        assertEquals(1, dao.update(c5));
        testDataSet.add(c5);
        testDataSet.remove(c1);
        assertEquals(testDataSet, getContractSet());
    }

    @Test
    final void testDelete() throws SQLException {
        ContractDao dao = getManager().getContractDao();
        truncateTable("contracts");

        // 空のテーブルに対してアップデート
        assertEquals(0, dao.delete(C10.getKey()));

        // テストデータを入れる
        Set<Contract> testDataSet = new HashSet<>(Arrays.asList(C10, C20, C30));
        dao.batchInsert(testDataSet);
        assertEquals(testDataSet, getContractSet());

        // 削除対象のレコードがないケース
        assertEquals(0, dao.delete(C40.getKey()));
        assertEquals(testDataSet, getContractSet());

        // 削除
        assertEquals(1, dao.delete(C20.getKey()));
        testDataSet.remove(C20);
        assertEquals(testDataSet, getContractSet());
    }


    @Test
    final void testGetContractsString() throws SQLException {
        ContractDao dao = getManager().getContractDao();
        truncateTable("contracts");

        // テーブルが空の時
        assertEquals(Collections.EMPTY_LIST, dao.getContracts("2"));

        // テーブルにレコード追加
        dao.batchInsert(Arrays.asList(C10, C20, C30, C31, C41, C40));

        // 検索結果が0件のとき
        List<Contract> actualt;
        actualt=  dao.getContracts("5");
        assertEquals(Collections.EMPTY_LIST, actualt);

        // 検索結果が1件のとき
        actualt=  dao.getContracts("1");
        assertEquals(Collections.singletonList(C10), actualt);

        // 検索結果が複数件のとき
        actualt=  dao.getContracts("4");
        assertEquals(Arrays.asList(C40, C41), actualt);
    }

    @Test
    final void testGetContractsDateDate() throws SQLException {
        ContractDao dao = getManager().getContractDao();
        truncateTable("contracts");

        // テーブルが空の時
        assertEquals(Collections.EMPTY_LIST,
                dao.getContracts(DateUtils.toDate("1918-01-01"), DateUtils.toDate("1978-05-01")));

        // テーブルにレコード追加
        dao.batchInsert(Arrays.asList(C10, C20, C30, C31, C41, C40));

        // 検索結果が0件(検索条件のendDateが、テストデータの最小のstartDateより小さい)
        List<Contract> actual;

        actual= dao.getContracts(DateUtils.toDate("1918-01-01"), DateUtils.toDate("1978-05-01"));
        assertEquals(Collections.EMPTY_LIST, actual);


        // endDateがnullのデータのみが返るケース

        actual= dao.getContracts(DateUtils.toDate("2118-01-01"), DateUtils.toDate("2118-05-01"));
        assertEquals(Arrays.asList(C20, C41), actual);


        // endDateの境界値
        actual=  dao.getContracts(DateUtils.toDate("1901-03-10"), DateUtils.toDate("2000-01-01"));
        assertEquals(Arrays.asList(C40), actual);

        actual=  dao.getContracts(DateUtils.toDate("1901-03-10"), DateUtils.toDate("2000-01-02"));
        assertEquals(Arrays.asList(C40), actual);

        actual= dao.getContracts(DateUtils.toDate("1901-03-10"), DateUtils.toDate("1999-12-31"));
        assertEquals(Collections.EMPTY_LIST, actual);

        // startDateの境界値
        actual= dao.getContracts(DateUtils.toDate("2001-03-11"), DateUtils.toDate("2001-03-15"));
        assertEquals(Arrays.asList(C40), actual);

        actual= dao.getContracts(DateUtils.toDate("2001-03-12"), DateUtils.toDate("2001-03-15"));
        assertEquals(Arrays.asList(C40), actual);

        actual= dao.getContracts(DateUtils.toDate("2001-03-13"), DateUtils.toDate("2001-03-15"));
        assertEquals(Collections.EMPTY_LIST, actual);
    }

    @Test
    final void testGetContracts() throws SQLException {
        ContractDao dao = getManager().getContractDao();
        truncateTable("contracts");
        Set<Contract> actualSet;

        // テーブルが空の時
        assertEquals(Collections.EMPTY_LIST, dao.getContracts());

        // テーブルにレコード追加
        dao.batchInsert(Arrays.asList(C10, C20, C30, C31, C40, C41));
        actualSet =  dao.getContracts().stream().collect(Collectors.toSet());
        Set<Contract> expectedSet = new HashSet<>(Arrays.asList(C10, C20, C30, C31, C40, C41));
        assertEquals(expectedSet , actualSet);
    }

    @Test
    final void testGetContract() throws SQLException {
        ContractDao dao = getManager().getContractDao();
        truncateTable("contracts");

        // テーブルにレコード追加
        dao.batchInsert(Arrays.asList(C10, C20, C30, C31, C40));

        // 存在するレコードのPKを指定したとき
        assertEquals(C10, dao.getContract(C10.getKey()));
        assertEquals(C20, dao.getContract(C20.getKey()));
        assertEquals(C30, dao.getContract(C30.getKey()));
        assertEquals(C31, dao.getContract(C31.getKey()));
        assertEquals(C40, dao.getContract(C40.getKey()));

        // 存在しないレPKを指定したとき
        assertNull(dao.getContract(C41.getKey()));
    }


    @Test
    final void testGetAllPhoneNumbers() throws SQLException {
        ContractDao dao = getManager().getContractDao();
        truncateTable("contracts");

        // テーブルが空の時
        assertEquals(Collections.EMPTY_LIST, dao.getAllPhoneNumbers());

        // テーブルにレコード追加
        dao.batchInsert(Arrays.asList(C20, C30, C40, C31, C10, C41));
        List<String> actual = dao.getAllPhoneNumbers();
        assertEquals(Arrays.asList("1", "2", "3", "3", "4", "4"), actual);
    }

    @Test
    final void testGetAllPrimaryKeys() throws SQLException {
        ContractDao dao = getManager().getContractDao();
        truncateTable("contracts");

        // テーブルが空の時
        assertEquals(Collections.EMPTY_LIST, dao.getAllPrimaryKeys());

        // テーブルにレコード追加
        List<Contract> list =Arrays.asList(C20, C30, C40, C31, C10, C41);
        List<Key> keyList = list.stream().map(c -> c.getKey()).collect(Collectors.toList());
        dao.batchInsert(list);
        List<Key> actual = dao.getAllPrimaryKeys();
        assertEquals(keyList, actual);
    }


    @Test
    final void testCount() {
        ContractDao dao = getManager().getContractDao();

        // テーブルが空の時
        assertEquals(0L, dao.count());

        // テーブルにレコード追加
        dao.batchInsert(Arrays.asList(C20, C30, C40, C31, C10, C41));
        assertEquals(6L, dao.count());
    }
}
