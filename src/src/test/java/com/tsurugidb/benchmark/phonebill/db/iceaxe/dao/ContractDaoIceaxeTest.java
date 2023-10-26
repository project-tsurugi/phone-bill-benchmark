package com.tsurugidb.benchmark.phonebill.db.iceaxe.dao;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.sql.Date;
import java.sql.SQLException;
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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.CreateTable;
import com.tsurugidb.benchmark.phonebill.app.billing.PhoneBill;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract.Key;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.IceaxeTestTools;
import com.tsurugidb.benchmark.phonebill.db.jdbc.Duration;
import com.tsurugidb.benchmark.phonebill.testdata.CreateTestData;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;

class ContractDaoIceaxeTest {
    private static final String ICEAXE_CONFIG_PATH = "src/test/config/iceaxe.properties";
    private static IceaxeTestTools testTools;
    private static PhoneBillDbManager manager;
    private static ContractDaoIceaxe dao;
    private static DdlIceaxe ddl;


    private static final Contract C10 = Contract.create("1", "2022-01-01", "2022-03-12", "dummy");
    private static final Contract C20 = Contract.create("2", "2022-02-01", null, "dummy2");
    private static final Contract C30 = Contract.create("3", "2020-01-01", "2021-03-12", "dummy3");
    private static final Contract C31 = Contract.create("3", "2022-04-01", "2022-09-12", "dummy4");
    private static final Contract C40 = Contract.create("4", "2000-01-01", "2001-03-12", "dummy5");
    private static final Contract C41 = Contract.create("4", "2002-04-01", null, "dummy6");



    @BeforeAll
    static void setUpBeforeClass() throws Exception {
        Config config = Config.getConfig(ICEAXE_CONFIG_PATH);
        testTools = new IceaxeTestTools(config);
        manager = testTools.getManager();
        dao = (ContractDaoIceaxe) manager.getContractDao();
        ddl = (DdlIceaxe) manager.getDdl();
    }

    @AfterAll
    static void tearDownAfterClass() throws Exception {
        testTools.close();
    }

    @BeforeEach
    void setUp() throws Exception {
        // 空のテーブルを作成する
        if (testTools.tableExists("contracts")) {
            testTools.execute(() -> ddl.dropTable("contracts"));
        }
        testTools.execute(ddl::createContractsTable);
    }

    @AfterEach
    void tearDown() throws Exception {
    }

    @Test
    final void testBatchInsert() {
        Set<Contract> expectedSet = new HashSet<>();
        List<Contract> list = new ArrayList<>();
        int[] ret;

        // 空のリストを渡したとき
        ret = testTools.execute(() -> {
            return dao.batchInsert(list);
        });
        ret = dao.batchInsert(list);
        assertEquals(0, ret.length);

        // 要素数 = 1のリスト
        Contract c = Contract.create("1", "2022-01-01", "2022-12-12", "dummy");
        expectedSet.add(c.clone());
        list.add(c.clone());

        ret = testTools.execute(() -> {
            return dao.batchInsert(list);
        });
        assertEquals(1, ret.length);
        assertEquals(expectedSet, testTools.getContractSet());

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

        ret = testTools.execute(() -> {
            return dao.batchInsert(list);
        });
        assertEquals(5, ret.length);
        assertEquals(expectedSet, testTools.getContractSet());
    }

    @Test
    final void testInsert() {
        Set<Contract> expectedSet = new HashSet<>();

        // 1件インサート
        Contract c = Contract.create("1", "2022-01-01", "2022-12-12", "dummy");
        expectedSet.add(c.clone());

        assertEquals(1, testTools.execute(() -> {
            return dao.insert(c);
        }));

        assertEquals(expectedSet, testTools.getContractSet());

        // Null可なデータが全てNullのデータをインサート
        c.setPhoneNumber("2");
        c.setEndDate((Date) null);


        assertEquals(1, testTools.execute(() -> {
            return dao.insert(c);
        }));
        expectedSet.add(c.clone());

        assertEquals(expectedSet, testTools.getContractSet());
    }

    @Test
    final void testUpdate() {
        Contract c1 = C10;
        Contract c2 = Contract.create("2", "2062-01-01", null, "dummy");
        Contract c3 = Contract.create("3", "2020-01-01", "2021-12-12", "dummy");
        Contract c4 = Contract.create("3", "2022-01-01", null, "dummy");
        Contract c5 = Contract.create("1", "2022-01-01", "2023-03-02", "dummyA");

        // 空のテーブルに対してアップデート

        assertEquals(0, testTools.execute(() -> {
            return dao.update(c1);
        }));

        // テストデータを入れる
        Set<Contract> testDataSet = new HashSet<>(Arrays.asList(c1, c2, c3));
        testTools.insertToContracts(testDataSet);
        assertEquals(testDataSet, testTools.getContractSet());

        // 更新対象のレコードがないケース
        assertEquals(0, testTools.execute(() -> {
            return dao.update(c4);
        }));
        assertEquals(testDataSet, testTools.getContractSet());

        // 同じ値で更新
        assertEquals(1, testTools.execute(() -> {
            return dao.update(c1);
        }));
        assertEquals(testDataSet, testTools.getContractSet());

        // キー値以外を全て更新
        assertEquals(1, testTools.execute(() -> {
            return dao.update(c5);
        }));
        testDataSet.add(c5);
        testDataSet.remove(c1);
        assertEquals(testDataSet, testTools.getContractSet());
    }

    @Test
    final void testDelete() throws SQLException {
        // 空のテーブルに対してdelete
        assertEquals(0, testTools.execute(() -> {
            return dao.delete(C10.getKey());
        }));

        // テストデータを入れる
        Set<Contract> testDataSet = new HashSet<>(Arrays.asList(C10, C20, C30));
        testTools.insertToContracts(testDataSet);
        assertEquals(testDataSet, testTools.getContractSet());

        // 削除対象のレコードがないケース
        assertEquals(0, testTools.execute(() -> {
            return dao.delete(C40.getKey());
        }));
        assertEquals(testDataSet, testTools.getContractSet());

        // 削除
        assertEquals(1, testTools.execute(() -> {
            return dao.delete(C20.getKey());
        }));
        testDataSet.remove(C20);
        assertEquals(testDataSet, testTools.getContractSet());
    }


    @Test
    final void testGetContractsString() {
        // テーブルが空の時
        assertEquals(Collections.EMPTY_LIST, testTools.execute(() -> {
            return dao.getContracts("2");
        }));

        // テーブルにレコード追加
        testTools.insertToContracts(C10, C20, C30, C31, C41, C40);

        // 検索結果が0件のとき
        List<Contract> actualt;
        actualt= testTools.execute(() -> {
            return dao.getContracts("5");
        });
        assertEquals(Collections.EMPTY_LIST, actualt);

        // 検索結果が1件のとき
        actualt = testTools.execute(() -> {
            return dao.getContracts("1");
        });
        assertEquals(Collections.singletonList(C10), actualt);

        // 検索結果が複数件のとき
        actualt = testTools.execute(() -> {
            return dao.getContracts("4");
        });
        assertEquals(Arrays.asList(C40, C41), actualt);
    }

    @Test
    final void testGetContractsDateDate() {
        // テーブルが空の時
        assertEquals(Collections.EMPTY_LIST, testTools.execute(() -> {
            return dao.getContracts(DateUtils.toDate("1918-01-01"), DateUtils.toDate("1978-05-01"));
        }));

        // テーブルにレコード追加
        testTools.insertToContracts(C10, C20, C30, C31, C41, C40);

        // 検索結果が0件(検索条件のendDateが、テストデータの最小のstartDateより小さい)
        List<Contract> actual;

        actual= testTools.execute(() -> {
            return dao.getContracts(DateUtils.toDate("1918-01-01"), DateUtils.toDate("1978-05-01"));
        });
        assertEquals(Collections.EMPTY_LIST, actual);


        // endDateがnullのデータのみが返るケース

        actual= testTools.execute(() -> {
            return dao.getContracts(DateUtils.toDate("2118-01-01"), DateUtils.toDate("2118-05-01"));
        });
        assertEquals(Arrays.asList(C20, C41), actual);


        // endDateの境界値
        actual= testTools.execute(() -> {
            return dao.getContracts(DateUtils.toDate("1901-03-10"), DateUtils.toDate("2000-01-01"));
        });
        assertEquals(Arrays.asList(C40), actual);

        actual= testTools.execute(() -> {
            return dao.getContracts(DateUtils.toDate("1901-03-10"), DateUtils.toDate("2000-01-02"));
        });
        assertEquals(Arrays.asList(C40), actual);

        actual= testTools.execute(() -> {
            return dao.getContracts(DateUtils.toDate("1901-03-10"), DateUtils.toDate("1999-12-31"));
        });
        assertEquals(Collections.EMPTY_LIST, actual);

        // startDateの境界値
        actual= testTools.execute(() -> {
            return dao.getContracts(DateUtils.toDate("2001-03-11"), DateUtils.toDate("2001-03-15"));
        });
        assertEquals(Arrays.asList(C40), actual);

        actual= testTools.execute(() -> {
            return dao.getContracts(DateUtils.toDate("2001-03-12"), DateUtils.toDate("2001-03-15"));
        });
        assertEquals(Arrays.asList(C40), actual);

        actual= testTools.execute(() -> {
            return dao.getContracts(DateUtils.toDate("2001-03-13"), DateUtils.toDate("2001-03-15"));
        });
        assertEquals(Collections.EMPTY_LIST, actual);



    }

    @Test
    final void testGetContracts() {
        Set<Contract> actualSet;

        // テーブルが空の時
        assertEquals(Collections.EMPTY_LIST, testTools.execute(() -> {
            return dao.getContracts();
        }));

        // テーブルにレコード追加
        testTools.insertToContracts(C10, C20, C30, C31, C40, C41);
        actualSet = testTools.execute(() -> {
            return dao.getContracts();
        }).stream().collect(Collectors.toSet());
        Set<Contract> expectedSet = new HashSet<>(Arrays.asList(C10, C20, C30, C31, C40, C41));
        assertEquals(expectedSet , actualSet);
    }

    @Test
    final void testGetContract() throws SQLException {
        // テーブルにレコード追加
        testTools.insertToContracts(C10, C20, C30, C31, C40);

        // 存在するレコードのPKを指定したとき
        assertEquals(C10, testTools.execute(() ->  {
            return dao.getContract(C10.getKey());
        }));
        assertEquals(C20, testTools.execute(() ->  {
            return dao.getContract(C20.getKey());
        }));
        assertEquals(C30, testTools.execute(() ->  {
            return dao.getContract(C30.getKey());
        }));
        assertEquals(C31, testTools.execute(() ->  {
            return dao.getContract(C31.getKey());
        }));
        assertEquals(C40, testTools.execute(() ->  {
            return dao.getContract(C40.getKey());
        }));

        // 存在しないレPKを指定したとき
        assertNull(testTools.execute(() ->  {
            return dao.getContract(C41.getKey());
        }));
    }


    /**
     * PostgreSQL版と同じ結果を返すことを確認する。
     * @throws IOException
     */
    @Test
    @Disabled("This test case was created to reproduce a bug and disabled because takes a long time to execute.")
    final void testGetContractsSameToPostgreSQL() throws Exception {
        // PostgreSQL版の結果を取得する
        Config configPostgres = Config.getConfig();
        configPostgres.numberOfContractsRecords = 1000;
        configPostgres.duplicatePhoneNumberRate=100;
        configPostgres.expirationDateRate = 400;
        configPostgres.noExpirationDateRate = 400;
        configPostgres.numberOfHistoryRecords = 0;
        new CreateTable().execute(configPostgres);
        new CreateTestData().execute(configPostgres);

        Duration d = PhoneBill.toDuration(configPostgres.targetMonth);

        PhoneBillDbManager managerPostgres = PhoneBillDbManager.createPhoneBillDbManager(configPostgres);
        Set<Contract> postgreSet = managerPostgres.execute(null, () -> {
            return managerPostgres.getContractDao().getContracts(d.getStatDate(), d.getEndDate());
        }).stream().collect(Collectors.toSet());

        // Iceaxe版の結果を取得する
        Config configIceaxe = Config.getConfig(ICEAXE_CONFIG_PATH);
        Config config = configPostgres.clone();
        config.url = configIceaxe.url;
        config.user = configIceaxe.user;
        config.password = configIceaxe.password;
        config.dbmsType = configIceaxe.dbmsType;
        new CreateTable().execute(config);
        new CreateTestData().execute(config);
        PhoneBillDbManager managerIceaxe = PhoneBillDbManager.createPhoneBillDbManager(config);
        Set<Contract> iceaxeSet = managerIceaxe.execute(TxOption.of(), () -> {
            return managerIceaxe.getContractDao().getContracts(d.getStatDate(), d.getEndDate());
        }).stream().collect(Collectors.toSet());

        // 比較
        for(Contract c: iceaxeSet) {
            if (postgreSet.contains(c)) {
                continue;
            }
            System.out.println("only in iceaxe; " + c);
        }
        for(Contract c: postgreSet) {
            if (iceaxeSet.contains(c)) {
                continue;
            }
            System.out.println("only in postgres; " + c);
        }
        assertEquals(postgreSet, iceaxeSet);

    }


    @Test
    final void testGetAllPhoneNumbers() {
        // テーブルが空の時
        assertEquals(Collections.EMPTY_LIST, testTools.execute(() -> {
            return dao.getAllPhoneNumbers();
        }));

        // テーブルにレコード追加
        testTools.insertToContracts(C20, C30, C40, C31, C10, C41);
        List<String> actual = testTools.execute(() -> {
            return dao.getAllPhoneNumbers();
        });
        assertEquals(Arrays.asList("1", "2", "3", "3", "4", "4"), actual);
    }

    @Test
    final void testGetAllPrimaryKeys() throws SQLException {
        // テーブルが空の時
        assertEquals(Collections.EMPTY_LIST, testTools.execute(() -> {
            return dao.getAllPrimaryKeys();
        }));

        // テーブルにレコード追加
        List<Contract> list =Arrays.asList(C20, C30, C40, C31, C10, C41);
        testTools.insertToContracts(list);
        Set<Key> keySet = list.stream().map(c -> c.getKey()).collect(Collectors.toSet());
        Set<Key> actual = testTools.execute(() -> {
            return new HashSet<>(dao.getAllPrimaryKeys());
        });
        assertEquals(keySet, actual);
    }

    @Test
    final void testCount() {
        // テーブルが空の時
        assertEquals(0L, testTools.execute(dao::count));

        // テーブルにレコード追加
        testTools.insertToContracts(C20, C30, C40, C31, C10, C41);
        assertEquals(6L, testTools.execute(dao::count));
    }
}
