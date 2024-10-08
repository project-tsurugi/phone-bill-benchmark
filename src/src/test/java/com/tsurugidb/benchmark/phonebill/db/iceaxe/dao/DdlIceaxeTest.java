/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 *
 */
package com.tsurugidb.benchmark.phonebill.db.iceaxe.dao;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.iceaxe.IceaxeTestTools;

/**
 *
 */
@Tag("tsurugi")
class DdlIceaxeTest {
    private static String ICEAXE_CONFIG_PATH = "src/test/config/iceaxe.properties";
    private static IceaxeTestTools testTools;
    private static PhoneBillDbManager manager;
    private static DdlIceaxe ddl;


    @BeforeAll
    static void setUpBeforeClass() throws IOException {
        Config config = Config.getConfig(ICEAXE_CONFIG_PATH);
        testTools = new IceaxeTestTools(config);
        manager = testTools.getManager();
        ddl = (DdlIceaxe) manager.getDdl();
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
