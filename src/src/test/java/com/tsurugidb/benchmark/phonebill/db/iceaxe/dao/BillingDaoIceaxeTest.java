package com.tsurugidb.benchmark.phonebill.db.iceaxe.dao;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.entity.Billing;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.IceaxeTestTools;
import com.tsurugidb.benchmark.phonebill.util.DateUtils;

class BillingDaoIceaxeTest {
    private static final String ICEAXE_CONFIG_PATH = "src/test/config/iceaxe.properties";

    Billing B1 = Billing.create("001", "2022-01-01", 0, 0, 0, "id0");
    Billing B2 = Billing.create("002", "2022-01-01", 0, 0, 0, "id0");
    Billing B3 = Billing.create("002", "2022-02-01", 0, 0, 0, "id0");
    Billing B4 = Billing.create("002", "2022-03-01", 0, 0, 0, "id0");


    private static IceaxeTestTools testTools;
    private static PhoneBillDbManager manager;
    private static BillingDaoIceaxe dao;
    private static DdlIceaxe ddl;


    @BeforeAll
    static void setUpBeforeClass() throws Exception {
        Config config = Config.getConfig(ICEAXE_CONFIG_PATH);
        testTools = new IceaxeTestTools(config);
        manager = testTools.getManager();
        dao = (BillingDaoIceaxe) manager.getBillingDao();
        ddl = (DdlIceaxe) manager.getDdl();
    }

    @AfterAll
    static void tearDownAfterClass() throws Exception {
        testTools.close();
    }

    @BeforeEach
    void setUp() throws Exception {
        // 空のテーブルを作成する
        if (testTools.tableExists("billing")) {
            testTools.execute(() -> ddl.dropTable("billing"));
        }
        testTools.execute(ddl::createBillingTable);
    }

    @AfterEach
    void tearDown() throws Exception {
    }

    @Test
    final void testInsert() {
        assertEquals(Collections.emptySet(), testTools.getBillingSet());


        testTools.execute(() -> {
            assertEquals(1, dao.insert(B1));
        });
        assertEquals(Collections.singleton(B1), testTools.getBillingSet());

        testTools.execute(() -> {
            assertEquals(1, dao.insert(B2));
            assertEquals(1, dao.insert(B3));
            assertEquals(1, dao.insert(B4));
        });
        assertEquals(new HashSet<>(Arrays.asList(B1, B2, B3, B4)), testTools.getBillingSet());
    }


    @Test
    final void testDelete() {
        testTools.insertToBilling(B1, B2, B3, B4);
        assertEquals(new HashSet<>(Arrays.asList(B1, B2, B3, B4)), testTools.getBillingSet());

        testTools.execute(() -> {
            assertEquals(0, dao.delete(DateUtils.toDate("2022-07-01")));
        });
        assertEquals(new HashSet<>(Arrays.asList(B1, B2, B3, B4)), testTools.getBillingSet());

        testTools.execute(() -> {
            assertEquals(2, dao.delete(DateUtils.toDate("2022-01-01")));
        });
        assertEquals(new HashSet<>(Arrays.asList(B3, B4)), testTools.getBillingSet());

        testTools.execute(() -> {
            assertEquals(1, dao.delete(DateUtils.toDate("2022-02-01")));
        });
        assertEquals(new HashSet<>(Arrays.asList(B4)), testTools.getBillingSet());

        testTools.execute(() -> {
            assertEquals(1, dao.delete(DateUtils.toDate("2022-03-01")));
        });
        assertEquals(Collections.emptySet(), testTools.getBillingSet());
    }

    @Test
    final void testGetBillings() {
        testTools.insertToBilling(B1, B2, B3, B4);
        HashSet<Billing> actual =
        testTools.execute(() -> {
            return new HashSet<>(dao.getBillings());
        });
        assertEquals(new HashSet<>(Arrays.asList(B1, B2, B3, B4)), actual);
    }

}
