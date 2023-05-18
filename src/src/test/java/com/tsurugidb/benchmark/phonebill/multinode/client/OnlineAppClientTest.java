package com.tsurugidb.benchmark.phonebill.multinode.client;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.TxLabel;
import com.tsurugidb.benchmark.phonebill.db.TxOption.Table;
import com.tsurugidb.benchmark.phonebill.db.dao.ContractDao;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;
import com.tsurugidb.benchmark.phonebill.online.AbstractOnlineApp;

class OnlineAppClientTest {

    @BeforeAll
    static void setUpBeforeClass() throws Exception {
    }

    @BeforeEach
    void setUp() throws Exception {
    }

    @Test
    final void testCreateStatusMessage() throws SQLException, IOException {


        List<AbstractOnlineApp> list = Arrays.asList(
                new TestApp("Name-C", 21),
                new TestApp("Name-A", 20),
                new TestApp("Name-B", 9),
                new TestApp("Name-A", 5),
                new TestApp("Name-C", 13),
                new TestApp("Name-C", 44)
                );


            Instant now = Instant.now();
            Instant start = now.minus(5518, ChronoUnit.MILLIS);


            String actual = OnlineAppClient.createStatusMessage(start, now, list);
            String expected = "uptime = 5.518 sec, exec count(Name-A = 25, Name-B = 9, Name-C = 78)";
            assertEquals(expected, actual);
    }

    /**
     * createStatusMessag()のテストに使うAbstractOnlineAppの実装
     */
    static class TestApp extends AbstractOnlineApp {
        private String baseName;
        private int count;

        public TestApp(String baseName, int count) throws SQLException, IOException {
            super( 0, Config.getConfig(), new Random());
            this.baseName = baseName;
            this.count = count;
        }

        @Override
        public int getExecCount() {
            return count;
        }

        @Override
        public String getBaseName() {
            return baseName;
        }

        @Override
        protected void createData(ContractDao contractDao, HistoryDao historyDao) {
        }

        @Override
        protected void updateDatabase(ContractDao contractDao, HistoryDao historyDao) {
        }

        @Override
        public TxLabel getTxLabel() {
            return TxLabel.TEST;
        }

        @Override
        public Table getWritePreserveTable() {
            return Table.HISTORY;
        }

        @Override
        protected void afterCommitSuccess() {
            // Nothing to do
        }
    }
}
