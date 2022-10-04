package com.tsurugidb.benchmark.phonebill.db.iceaxe;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.iceaxe.result.TgEntityResultMapping;
import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.result.TsurugiResultSet;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementQuery0;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementQuery1;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementUpdate1;
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
			// TODO 自動生成されたメソッド・スタブ
			return null;
		}
	});
	private static final IOException IO_EXCEPTION = new IOException();



	private static PhoneBillDbManagerIceaxe manager;
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

	@Test
	final void testCreatePreparedStatement() {
		assertThrows(UncheckedIOException.class, () -> utils.createPreparedStatement(null, null));
	}

	@Test
	final void testCreatePreparedQuery() {
		assertThrows(UncheckedIOException.class, () -> utils.createPreparedQuery(null));
		assertThrows(UncheckedIOException.class, () -> utils.createPreparedQuery(null, (TgEntityResultMapping<History>)null));
		assertThrows(UncheckedIOException.class, () -> utils.createPreparedQuery(null, (TgParameterMapping<TgParameterList>)null));
		assertThrows(UncheckedIOException.class, () -> utils.createPreparedQuery(null, null, null));
	}

	@Test
	final void testExecute() {
		assertThrows(UncheckedIOException.class,
				() -> utils.execute(new TsurugiPreparedStatementQuery0<TsurugiResultEntity>(session, null, null) {
					@Override
					public TsurugiResultSet<TsurugiResultEntity> execute(TsurugiTransaction transaction)
							throws IOException, TsurugiTransactionException {
						throw IO_EXCEPTION;
					}
				}));
		assertThrows(TsurugiTransactionRuntimeException.class,
				() -> utils.execute(new TsurugiPreparedStatementQuery0<TsurugiResultEntity>(session, null, null) {
					@Override
					public TsurugiResultSet<TsurugiResultEntity> execute(TsurugiTransaction transaction)
							throws IOException, TsurugiTransactionException {
						throw TSURUGI_TRANSACTION_EXCEPTION;
					}
				}));

		assertThrows(UncheckedIOException.class,
				() -> utils.execute(new TsurugiPreparedStatementQuery1<TgParameterList, History>(session, null, null, null) {

					@Override
					public TsurugiResultSet<History> execute(TsurugiTransaction transaction, TgParameterList parameter)
							throws IOException, TsurugiTransactionException {
						throw IO_EXCEPTION;
					}
				}, null));

		assertThrows(TsurugiTransactionRuntimeException.class,
				() -> utils.execute(new TsurugiPreparedStatementQuery1<TgParameterList, History>(session, null, null, null) {

					@Override
					public TsurugiResultSet<History> execute(TsurugiTransaction transaction, TgParameterList parameter)
							throws IOException, TsurugiTransactionException {
						throw TSURUGI_TRANSACTION_EXCEPTION;
					}
				}, null));
	}

	@Test
	final void testExecuteAndGetCount() {

		TsurugiPreparedStatementUpdate1<History> psThrowIOException = new TsurugiPreparedStatementUpdate1<>(session, null, null) {
			@Override
		    public int executeAndGetCount(TsurugiTransaction transaction, History h) throws IOException, TsurugiTransactionException {
				throw IO_EXCEPTION;
		    }
		};

		TsurugiPreparedStatementUpdate1<History> psThrowTsurugiTransactionException = new TsurugiPreparedStatementUpdate1<>(session, null, null) {
			@Override
		    public int executeAndGetCount(TsurugiTransaction transaction, History h) throws IOException, TsurugiTransactionException {
				throw TSURUGI_TRANSACTION_EXCEPTION;
		    }
		};

		assertThrows(UncheckedIOException.class, () -> utils.executeAndGetCount(psThrowIOException, (History)null));
		assertThrows(TsurugiTransactionRuntimeException.class, () -> utils.executeAndGetCount(psThrowTsurugiTransactionException, (History)null));
		List<History> list = Collections.singletonList(History.create("1", "2", "C", "2022-01-01 00:00:00.000", 10, null, 0));
		assertThrows(UncheckedIOException.class, () -> utils.executeAndGetCount(psThrowIOException, list));
		assertThrows(TsurugiTransactionRuntimeException.class, () -> utils.executeAndGetCount(psThrowTsurugiTransactionException, list));
	}

	private static class TestManager extends PhoneBillDbManagerIceaxe {
		TestSession session;

		public TestManager(Config config) {
			super(config);
			session = new TestSession(super.getSession());
		}

		@Override
		public TsurugiSession getSession() {
			return session;
		}
	}

	private static class TestSession extends TsurugiSession  {

		public TestSession(TsurugiSession session) {
			super(session.getSessionInfo() , null, null);
		}

		@Override
	    public <P> TsurugiPreparedStatementUpdate1<P> createPreparedStatement(String sql, TgParameterMapping<P> parameterMapping) throws IOException {
	        throw new IOException();
	    }

		@Override
	    public <R> TsurugiPreparedStatementQuery0<R> createPreparedQuery(String sql, TgResultMapping<R> resultMapping) throws IOException {
	        throw new IOException();
	    }

		@Override
	    public <P> TsurugiPreparedStatementQuery1<P, TsurugiResultEntity> createPreparedQuery(String sql, TgParameterMapping<P> parameterMapping) throws IOException {
	        throw new IOException();
	    }

		@Override
	    public <P, R> TsurugiPreparedStatementQuery1<P, R> createPreparedQuery(String sql, TgParameterMapping<P> parameterMapping, TgResultMapping<R> resultMapping) throws IOException {
	        throw new IOException();
	    }
	}
}
