package com.tsurugidb.benchmark.phonebill.db.iceaxe;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedQuery;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedStatement;
import com.tsurugidb.iceaxe.sql.TsurugiSqlQuery;
import com.tsurugidb.iceaxe.sql.TsurugiSqlStatement;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiQueryResult;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.sql.result.mapping.TgEntityResultMapping;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRuntimeException;
import com.tsurugidb.tsubakuro.exception.DiagnosticCode;
import com.tsurugidb.tsubakuro.exception.ServerException;

/**
 * IceaxeUtilsのテストクラス.
 * <br>
 * 正常系は各テストクラスのテストでカバーされるので、本クラスでは異常系のテストのみ実行する。
 */
class IceaxeUtilsTest {
    private static final String ICEAXE_CONFIG_PATH = "src/test/config/iceaxe.properties";
    private static final TsurugiTransactionException TSURUGI_TRANSACTION_EXCEPTION = new TsurugiTransactionException(new ServerException() {
        @Override
        public DiagnosticCode getDiagnosticCode() {
            return null;
        }
    });
    private static final IOException IO_EXCEPTION = new IOException();



    private static TestManager manager;
    private static TsurugiSession session;
    private static IceaxeUtils utils;

    @BeforeAll
    static void setUpBeforeClass() throws Exception {
        Config config = Config.getConfig(ICEAXE_CONFIG_PATH);
        manager = new TestManager(config);
        session = manager.getSession();
        utils = new IceaxeUtils(manager);
    }

    @AfterAll
    static void tearDownAfterClass() throws Exception {
        manager.close();
    }

    @BeforeEach
    void setUp() {
        manager.transaction = null;
    }

    @Test
    final void testCreatePreparedStatement() {
        assertThrows(UncheckedIOException.class, () -> utils.createPreparedStatement(null, null));
        assertThrows(UncheckedIOException.class, () -> utils.createPreparedStatement(null));
    }

    @Test
    final void testCreatePreparedQuery() {
        assertThrows(UncheckedIOException.class, () -> utils.createPreparedQuery(null));
        assertThrows(UncheckedIOException.class, () -> utils.createPreparedQuery(null, (TgEntityResultMapping<History>)null));
        assertThrows(UncheckedIOException.class, () -> utils.createPreparedQuery(null, (TgParameterMapping<TgBindParameters>)null));
        assertThrows(UncheckedIOException.class, () -> utils.createPreparedQuery(null, null, null));
    }

    @Test
    final void testExecute() {
        manager.execute(TxOption.of(), () -> {
            assertThrows(UncheckedIOException.class,
                    () -> utils.execute(new TsurugiSqlQuery<TsurugiResultEntity>(session, "", null) {
                        @Override
                        public TsurugiQueryResult<TsurugiResultEntity> execute(TsurugiTransaction transaction)
                                throws IOException, TsurugiTransactionException {
                            throw IO_EXCEPTION;
                        }
                    }));
            assertThrows(TsurugiTransactionRuntimeException.class,
                    () -> utils.execute(new TsurugiSqlQuery<TsurugiResultEntity>(session, "", null) {
                        @Override
                        public TsurugiQueryResult<TsurugiResultEntity> execute(TsurugiTransaction transaction)
                                throws IOException, TsurugiTransactionException {
                            throw TSURUGI_TRANSACTION_EXCEPTION;
                        }
                    }));

            assertThrows(UncheckedIOException.class, () -> utils
                    .execute(new TsurugiSqlPreparedQuery<TgBindParameters, History>(session, "", null, null, null) {

                        @Override
                        public TsurugiQueryResult<History> execute(TsurugiTransaction transaction,
                                TgBindParameters parameter) throws IOException, TsurugiTransactionException {
                            throw IO_EXCEPTION;
                        }
                    }, null));

            assertThrows(TsurugiTransactionRuntimeException.class, () -> utils
                    .execute(new TsurugiSqlPreparedQuery<TgBindParameters, History>(session, "", null, null, null) {

                        @Override
                        public TsurugiQueryResult<History> execute(TsurugiTransaction transaction,
                                TgBindParameters parameter) throws IOException, TsurugiTransactionException {
                            throw TSURUGI_TRANSACTION_EXCEPTION;
                        }
                    }, null));
        });
    }

    @Test
    final void testExecuteAndGetCount() throws IOException {
        List<History> list = Collections
                .singletonList(History.create("1", "2", "C", "2022-01-01 00:00:00.000", 10, null, 0));

        manager.transaction = new TsurugiTransaction(session, null, null) {
            @Override
            public <P> int executeAndGetCount(TsurugiSqlPreparedStatement<P> ps, P parameter) throws IOException, TsurugiTransactionException {
                throw IO_EXCEPTION;
            }

            @Override
            public int executeAndGetCount(TsurugiSqlStatement ps) throws IOException, TsurugiTransactionException {
                throw IO_EXCEPTION;
            }
        };
        assertThrows(UncheckedIOException.class,
                () -> utils.executeAndGetCount((TsurugiSqlStatement) null));
        assertThrows(UncheckedIOException.class,
                () -> utils.executeAndGetCount((TsurugiSqlPreparedStatement<History>) null, (History) null));
        assertThrows(UncheckedIOException.class,
                () -> utils.executeAndGetCount((TsurugiSqlPreparedStatement<History>) null, list));

        manager.transaction = new TsurugiTransaction(session, null, null) {
            @Override
            public <P> int executeAndGetCount(TsurugiSqlPreparedStatement<P> ps, P parameter)
                    throws IOException, TsurugiTransactionException {
                throw TSURUGI_TRANSACTION_EXCEPTION;
            }

            @Override
            public int executeAndGetCount(TsurugiSqlStatement ps)
                    throws IOException, TsurugiTransactionException {
                throw TSURUGI_TRANSACTION_EXCEPTION;
            }

        };
        assertThrows(TsurugiTransactionRuntimeException.class,
                () -> utils.executeAndGetCount((TsurugiSqlStatement) null));
        assertThrows(TsurugiTransactionRuntimeException.class,
                () -> utils.executeAndGetCount((TsurugiSqlPreparedStatement<History>) null, (History) null));
        assertThrows(TsurugiTransactionRuntimeException.class,
                () -> utils.executeAndGetCount((TsurugiSqlPreparedStatement<History>) null, list));
    }

    private static class TestManager extends PhoneBillDbManagerIceaxe {
        TsurugiTransaction transaction = null;

        public TestManager(Config config) {
            super(config);
        }

        @Override
        public TsurugiSession getSession() {
            return new TestSession(super.getSession());
        }

        @Override
        public TsurugiTransaction getCurrentTransaction() {
            return transaction == null ?  super.getCurrentTransaction() : transaction;
        }
    }

    private static class TestSession extends TsurugiSession  {

        public TestSession(TsurugiSession session) {
            super(null, session.getSessionOption());
        }

        @Override
        public <P> TsurugiSqlPreparedStatement<P> createStatement(String sql, TgParameterMapping<P> parameterMapping) throws IOException {
            throw new IOException();
        }

        @Override
        public TsurugiSqlStatement createStatement(String sql) throws IOException {
            throw new IOException();
        }

        @Override
        public <R> TsurugiSqlQuery<R> createQuery(String sql, TgResultMapping<R> resultMapping) throws IOException {
            throw new IOException();
        }

        @Override
        public <P> TsurugiSqlPreparedQuery<P, TsurugiResultEntity> createQuery(String sql, TgParameterMapping<P> parameterMapping) throws IOException {
            throw new IOException();
        }

        @Override
        public <P, R> TsurugiSqlPreparedQuery<P, R> createQuery(String sql, TgParameterMapping<P> parameterMapping, TgResultMapping<R> resultMapping) throws IOException {
            throw new IOException();
        }
    }
}
