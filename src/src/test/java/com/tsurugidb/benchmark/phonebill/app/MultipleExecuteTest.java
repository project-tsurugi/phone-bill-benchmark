package com.tsurugidb.benchmark.phonebill.app;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.dao.Ddl;

class MultipleExecuteTest {
    private static final String ICEAXE_CONFIG_PATH = "src/test/config/iceaxe.properties";

    @Test
    final void testNeedCreateTestData() throws Exception {
        // デフォルトのDB(PostgreSQLでのテスト
        testNeedCreateTestDataSub(Config.getConfig());
        // Iceaxeでのテスト
        testNeedCreateTestDataSub(Config.getConfig(ICEAXE_CONFIG_PATH));
    }

    private void testNeedCreateTestDataSub(Config config) throws Exception {
        Config testConfig = config.clone();
        new CreateTable().execute(config);

        testConfig.numberOfContractsRecords = 0;
        testConfig.numberOfHistoryRecords = 0;

        MultipleExecute multipleExecute = new MultipleExecute();
        assertFalse(multipleExecute.needCreateTestData(testConfig));


        try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
            Ddl ddl = manager.getDdl();

            // history
            manager.execute(TxOption.of(), () -> ddl.dropTable("history"));
            assertTrue(multipleExecute.needCreateTestData(testConfig));
            manager.execute(TxOption.of(), ddl::createHistoryTable);
            assertFalse(multipleExecute.needCreateTestData(testConfig));

            // billing
            manager.execute(TxOption.of(), () -> ddl.dropTable("billing"));
            assertTrue(multipleExecute.needCreateTestData(testConfig));
            manager.execute(TxOption.of(), ddl::createBillingTable);
            assertFalse(multipleExecute.needCreateTestData(testConfig));


            // contracts
            manager.execute(TxOption.of(), () -> ddl.dropTable("contracts"));
            assertTrue(multipleExecute.needCreateTestData(testConfig));
            manager.execute(TxOption.of(), ddl::createContractsTable);
            assertFalse(multipleExecute.needCreateTestData(testConfig));
        }

        testConfig.numberOfContractsRecords = 1;
        assertTrue(multipleExecute.needCreateTestData(testConfig));
        testConfig.numberOfContractsRecords = 0;
        assertFalse(multipleExecute.needCreateTestData(testConfig));

        testConfig.numberOfHistoryRecords = 1;
        assertTrue(multipleExecute.needCreateTestData(testConfig));
        testConfig.numberOfHistoryRecords = 0;
        assertFalse(multipleExecute.needCreateTestData(testConfig));
    }


    @Test
    final void testCreateOnlineAppReport() throws IOException {
        Config config = Config.getConfig();
        MultipleExecute execute = new MultipleExecute();
        String report = execute.createOnlineAppReport(config, "Example");

        List<String> lines = splitToLines(report);
        assertEquals(12, lines.size());
        assertEquals("## Example", lines.get(0));
        assertEquals("", lines.get(1));
        assertEquals("| application    | Threads | tpm/thread | records/tx | succ | occ-try | occ-abort | occ-succ | occ<br>abandoned<br>retry | ltx-try | ltx-abort | ltx-succ |ltx<br>abandoned<br>retry|", lines.get(2));

        assertEquals("|----------------|--------:|-----------:|-----------:|-----:|--------:|----------:|---------:|--------------------------:|--------:|----------:|---------:|------------------------:|", lines.get(3));
        assertEquals("|Master Delete/Insert|1|0|1|0|0|0|0|0|0|0|0|0|", lines.get(4));
        assertEquals("|Master Update|1|0|1|0|0|0|0|0|0|0|0|0|", lines.get(5));
        assertEquals("|History Insert|1|0|1|0|0|0|0|0|0|0|0|0|", lines.get(6));
        assertEquals("|History Update|1|0|1|0|0|0|0|0|0|0|0|0|", lines.get(7));

    }


    List<String> splitToLines(String str) {
        return Arrays.asList(str.split("\r\n|\r|\n"));

    }
}
