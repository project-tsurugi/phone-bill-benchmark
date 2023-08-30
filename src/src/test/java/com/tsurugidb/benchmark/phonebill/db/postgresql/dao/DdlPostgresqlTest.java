package com.tsurugidb.benchmark.phonebill.db.postgresql.dao;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.dao.Ddl;
import com.tsurugidb.benchmark.phonebill.db.jdbc.PhoneBillDbManagerJdbc;

class DdlPostgresqlTest {
    private static PhoneBillDbManager manager;
    private static Connection conn;

    @BeforeAll
    static void setUpBeforeClass() throws Exception {
        manager = PhoneBillDbManager.createPhoneBillDbManager(Config.getConfig());
        conn = ((PhoneBillDbManagerJdbc)manager).getConnection();
        conn.setAutoCommit(false);
    }

    @AfterAll
    static void tearDownAfterClass() throws Exception {
        conn.close();
        manager.close();
    }

    @Test
    final void testTableExists() {
        Ddl ddl = manager.getDdl();
        ddl.dropTables();
        assertFalse(ddl.tableExists("history"));
        assertFalse(ddl.tableExists("contracts"));
        assertFalse(ddl.tableExists("billing"));
        assertFalse(ddl.tableExists("BILLING"));
        assertFalse(ddl.tableExists("biLLing"));

        ddl.createBillingTable();
        assertFalse(ddl.tableExists("history"));
        assertFalse(ddl.tableExists("contracts"));
        assertTrue(ddl.tableExists("billing"));

        ddl.createContractsTable();
        assertFalse(ddl.tableExists("history"));
        assertTrue(ddl.tableExists("contracts"));
        assertTrue(ddl.tableExists("billing"));

        ddl.createHistoryTable();
        assertTrue(ddl.tableExists("history"));
        assertTrue(ddl.tableExists("contracts"));
        assertTrue(ddl.tableExists("billing"));
    }

}
