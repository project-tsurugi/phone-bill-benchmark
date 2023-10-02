package com.tsurugidb.benchmark.phonebill.db.iceaxe.dao;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.Config.DbmsType;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.IceaxeTestTools;

class DdlIceaxeSurrogateKeyTest {
    private static String ICEAXE_CONFIG_PATH = "src/test/config/iceaxe.properties";
    private static IceaxeTestTools testTools;
    private static PhoneBillDbManager manager;
    private static DdlIceaxeSurrogateKey ddl;


    @BeforeAll
    static void setUpBeforeClass() throws IOException {
        Config config = Config.getConfig(ICEAXE_CONFIG_PATH);
        config.dbmsType = DbmsType.ICEAXE_SURROGATE_KEY;
        testTools = new IceaxeTestTools(config);
        manager = testTools.getManager();
        ddl = (DdlIceaxeSurrogateKey) manager.getDdl();
    }

    @AfterAll
    static void tearDownAfterClass() {
        testTools.close();
    }

    /**
     * {@link com.tsurugidb.benchmark.phonebill.db.iceaxe.dao.DdlIceaxe#dropTable(java.lang.String)} のためのテスト・メソッド。
     */
    @Test
    final void testDropTable() {
        // テーブルを作成
        if (!testTools.tableExists("billing")) {
            testTools.execute(ddl::createBillingTable);
        }
        assertTrue(testTools.tableExists("billing"));
        if (!testTools.tableExists("contracts")) {
            testTools.execute(ddl::createContractsTable);
        }
        assertTrue(testTools.tableExists("contracts"));
        if (!testTools.tableExists("history")) {
            testTools.execute(ddl::createHistoryTable);
        }
        assertTrue(testTools.tableExists("history"));
        // dropTable()でテーブルが削除されることを確認
        testTools.execute(ddl::dropTables);
        assertFalse(testTools.tableExists("contracts"));
        assertFalse(testTools.tableExists("billing"));
        assertFalse(testTools.tableExists("history"));


        // 存在しないテーブルをdropしてもエラーにならない
        testTools.execute(ddl::dropTables);
    }


    /**
     * {@link com.tsurugidb.benchmark.phonebill.db.iceaxe.dao.DdlIceaxe#createHistoryTable()} のためのテスト・メソッド。
     */
    @Test
    final void testCreateHistoryTable() {
        if (testTools.tableExists("history")) {
            testTools.execute(()->{
                ddl.dropTable("history");
            });
        }
        assertFalse(testTools.tableExists("history"));
        testTools.execute(ddl::createHistoryTable);
        assertTrue(testTools.tableExists("history"));

    }

    /**
     * {@link com.tsurugidb.benchmark.phonebill.db.iceaxe.dao.DdlIceaxe#createBillingTable()} のためのテスト・メソッド。
     */
    @Test
    final void testCreateBillingTable() {
        if (testTools.tableExists("billing")) {
            testTools.execute(()->{
            ddl.dropTable("billing");
            });
        }
        assertFalse(testTools.tableExists("billing"));
        testTools.execute(ddl::createBillingTable);
        assertTrue(testTools.tableExists("billing"));
    }

    /**
     * {@link com.tsurugidb.benchmark.phonebill.db.iceaxe.dao.DdlIceaxe#createContractsTable()} のためのテスト・メソッド。
     */
    @Test
    final void testCreateContractsTable() {
        if (testTools.tableExists("contracts")) {
            testTools.execute( ()->{
            ddl.dropTable("contracts");
            });
        }
        assertFalse(testTools.tableExists("contracts"));
        testTools.execute( ddl::createContractsTable);
        assertTrue(testTools.tableExists("contracts"));
    }

    /**
     * {@link com.tsurugidb.benchmark.phonebill.db.iceaxe.dao.DdlIceaxe#createIndexes()} のためのテスト・メソッド。
     */
    @Test
    @Disabled
    final void testCreateIndexes() {
        fail("まだ実装されていません");
    }

    /**
     * {@link com.tsurugidb.benchmark.phonebill.db.iceaxe.dao.DdlIceaxe#updateStatistics()} のためのテスト・メソッド。
     */
    @Test
    @Disabled
    final void testUpdateStatistics() {
        fail("まだ実装されていません");
    }

    /**
     * {@link com.tsurugidb.benchmark.phonebill.db.iceaxe.dao.DdlIceaxe#afterLoadData()} のためのテスト・メソッド。
     */
    @Test
    @Disabled
    final void testAfterLoadData() {
        fail("まだ実装されていません");
    }

    /**
     * {@link com.tsurugidb.benchmark.phonebill.db.iceaxe.dao.DdlIceaxe#prepareLoadData()} のためのテスト・メソッド。
     */
    @Test
    @Disabled
    final void testPrepareLoadData() {
        fail("まだ実装されていません");
    }

    @Test
    final void testTableExists() {
        testTools.execute(ddl::dropTables);
        assertFalse(ddl.tableExists("history"));
        assertFalse(ddl.tableExists("contracts"));
        assertFalse(ddl.tableExists("billing"));

        testTools.execute(() -> {
            ddl.createBillingTable();
        });
        assertFalse(ddl.tableExists("history"));
        assertFalse(ddl.tableExists("contracts"));
        assertTrue(ddl.tableExists("billing"));

        testTools.execute(() -> {
            ddl.createContractsTable();
        });
        assertFalse(ddl.tableExists("history"));
        assertTrue(ddl.tableExists("contracts"));
        assertTrue(ddl.tableExists("billing"));

        testTools.execute(() -> {
            ddl.createHistoryTable();
        });
        assertTrue(ddl.tableExists("history"));
        assertTrue(ddl.tableExists("contracts"));
        assertTrue(ddl.tableExists("billing"));
    }

}
