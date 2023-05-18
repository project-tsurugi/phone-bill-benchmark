package com.tsurugidb.benchmark.phonebill.online;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.entity.Contract;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.IceaxeTestTools;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.dao.DdlIceaxe;
import com.tsurugidb.benchmark.phonebill.util.TestRandom;

class MasterDeleteInsertAppIceaxeTest {
    private static String ICEAXE_CONFIG = "src/test/config/iceaxe.properties";

    private static IceaxeTestTools testTools;
    private static PhoneBillDbManager manager;
    private static DdlIceaxe ddl;
    private static Config config;


    private static final Contract C10 = Contract.create("1", "2022-01-01", "2022-03-12", "dummy");
    private static final Contract C20 = Contract.create("2", "2022-02-01", null, "dummy2");
    private static final Contract C30 = Contract.create("3", "2020-01-01", "2021-03-12", "dummy3");
    private static final Contract C31 = Contract.create("3", "2022-04-01", "2022-09-12", "dummy4");
    private static final Contract C40 = Contract.create("4", "2000-01-01", "2001-03-12", "dummy5");
    private static final Contract C41 = Contract.create("4", "2002-04-01", null, "dummy6");

    @BeforeAll
    static void setUpBeforeClass() throws Exception {
        config = Config.getConfig(ICEAXE_CONFIG);
        testTools = new IceaxeTestTools(config);
        manager = testTools.getManager();
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

    @Test
    void test() throws Exception {
        // テーブルにレコード追加
        List<Contract> list = Arrays.asList(C10, C20, C30, C31, C40, C41);
        testTools.insertToContracts(list);

        // テスト用のオンラインアプリケーションを初期化する
        TestRandom random = new TestRandom();
        random.setValues(list.stream().map(c -> Integer.valueOf(0)).collect(Collectors.toList()));
        RandomKeySelector<Contract.Key> keySelector = new RandomKeySelector<>(
                list.stream().map(c -> c.getKey()).collect(Collectors.toList()),
                random, 0d
                );
        MasterDeleteInsertApp app = new MasterDeleteInsertApp(config, random, keySelector);


        random.setValues(1, 0, 3);
        try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
            // 乱数により選択されたコードが削除される
            Set<Contract> expectedSet = new HashSet<>(list);
            expectedSet.remove(list.get(1));
            app.exec(manager);
            assertEquals(expectedSet , testTools.getContractSet());

            // 削除したレコードがinsertされて復活する
            expectedSet.add(list.get(1));
            app.exec(manager);
            assertEquals(expectedSet , testTools.getContractSet());

            // 乱数により選択されたコードが削除される
            expectedSet.remove(list.get(0));
            app.exec(manager);
            assertEquals(expectedSet , testTools.getContractSet());

            // 削除したレコードがinsertされて復活する
            expectedSet.add(list.get(0));
            app.exec(manager);
            assertEquals(expectedSet , testTools.getContractSet());

            // 乱数により選択されたコードが削除される
            expectedSet.remove(list.get(3));
            app.exec(manager);
            assertEquals(expectedSet , testTools.getContractSet());

            // 削除したレコードがinsertされて復活する
            expectedSet.add(list.get(3));
            app.exec(manager);
            assertEquals(expectedSet , testTools.getContractSet());
        }
    }
}
