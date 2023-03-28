package com.tsurugidb.benchmark.phonebill.db.jdbc;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PSQLException;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.app.Config.IsolationLevel;
import com.tsurugidb.benchmark.phonebill.db.AbstractPhoneBillDbManagerTest;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager.SessionHoldingType;
import com.tsurugidb.benchmark.phonebill.db.TxOption;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;
import com.tsurugidb.benchmark.phonebill.db.entity.History;
import com.tsurugidb.benchmark.phonebill.db.jdbc.dao.HistoryDaoJdbc;
import com.tsurugidb.benchmark.phonebill.db.postgresql.PhoneBillDbManagerPostgresql;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;

class PhoneBillDbManagerJdbcTest extends  AbstractPhoneBillDbManagerTest{
	private int tryCount = 0;

	@Test
	final void testExecuteTgTmSettingRunnable() throws Exception {
		Config config = Config.getConfig();
		createTestData(config);

		// 正常系
		try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
			List<History> before = getHistories();
			List<History> after = new ArrayList<>(before);

			// 1レコード更新
			final History h = before.get(0).clone();
			h.setCharge(h.getCharge() == null ? 300 : h.getCharge() + 300);
			after.set(0, h);
			manager.execute(TxOption.of(), () -> {
				HistoryDao dao = manager.getHistoryDao();
				dao.update(h);
				// コミット前は別コネクションに更新が反映されない
				try {
					assertIterableEquals(before, getHistories());
					assertIterableEquals(after, getHistories(manager));
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
			});
			// コミット後に別コネクションでも更新が反映されている
			assertIterableEquals(after, getHistories());
			assertIterableEquals(after, getHistories(manager));
		}

		// ロールバックが起きるケース
		try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
			// もう一度更新
			List<History> before = getHistories();
			List<History> after = new ArrayList<>(before);
			History h = before.get(0).clone();
			after.set(0, h);
			try {
				manager.execute(TxOption.of(), () -> {
					h.setCharge(h.getCharge() + 500);
					HistoryDao dao = manager.getHistoryDao();
					dao.update(h);
					// コミット前は別コネクションに更新が反映されない
					try {
						assertIterableEquals(before, getHistories());
						assertIterableEquals(after, getHistories(manager));
					} catch (SQLException e) {
						throw new RuntimeException(e);
					}
					dao.throwRuntimeException(); // Exceptionをスローしてロールバックを発生させる
				});
			} catch (RuntimeException e) {
				// ロールバックを発生させるためのRuntimeException => 無視する
			}
			// ロールバック後にもとに戻っている
			assertIterableEquals(before, getHistories());
			assertIterableEquals(before, getHistories(manager));
		}

		// リトライ可能な例外でリトライされるケース
		try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
			List<History> before = getHistories();
			List<History> after = new ArrayList<>(before);

			// 1レコード更新
			final History h = before.get(0).clone();
			h.setCharge(h.getCharge() == null ? 300 : h.getCharge() + 300);
			after.set(0, h);
			tryCount = 0;
			manager.execute(TxOption.of(10), () -> {
				if (++tryCount < 4) {
					throw new RuntimeException(new SerializationFailureException("40001"));
				}
				HistoryDao dao = manager.getHistoryDao();
				dao.update(h);
				// コミット前は別コネクションに更新が反映されない
				try {
					assertIterableEquals(before, getHistories());
					assertIterableEquals(after, getHistories(manager));
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
			});
			// コミット後に別コネクションでも更新が反映されている
			manager.commit();
			assertIterableEquals(after, getHistories());
			assertIterableEquals(after, getHistories(manager));
			assertEquals(4, tryCount);
		}

		// ロールバックに失敗するケース
		RuntimeException testException = new RuntimeException();
		try (PhoneBillDbManagerJdbc manager = new DbManagerRollbackFailure(config, testException)) {
			tryCount = 0;
			RuntimeException e = assertThrows(RuntimeException.class,
					() -> manager.execute(TxOption.of(), () -> {
						if (++tryCount <= 1) {
							throw new RuntimeException(new SerializationFailureException("40001"));
						}
						assert false; // ロールバックに失敗した場合はリトライされないのでここに制御がこないはず。
					}));
			assertEquals(SerializationFailureException.class, e.getCause().getClass());
			assertEquals(testException, e.getSuppressed()[0]);
		}
	}


	@Test
	final void testExecuteTgTmSettingSupplierOfT() throws Exception {
		Config config = Config.getConfig();
		createTestData(config);

		// 正常系
		try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
			List<History> before = getHistories();
			List<History> after = new ArrayList<>(before);

			// 1レコード更新
			final History h = before.get(0).clone();
			h.setCharge(h.getCharge() == null ? 300 : h.getCharge() + 300);
			after.set(0, h);
			HistoryDao dao = manager.getHistoryDao();
			manager.execute(TxOption.of(), () -> dao.update(h));
			// コミット後に別コネクションでも更新が反映されている
			assertIterableEquals(after, getHistories());
			assertIterableEquals(after, getHistories(manager));
		}

		// ロールバックが起きるケース
		try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
			// もう一度更新
			List<History> before = getHistories();
			List<History> after = new ArrayList<>(before);
			History h = before.get(0).clone();
			after.set(0, h);
			try {
				manager.execute(TxOption.of(), () -> {
					h.setCharge(h.getCharge() + 500);
					HistoryDao dao = manager.getHistoryDao();
					dao.update(h);
					// コミット前は別コネクションに更新が反映されない
					try {
						assertIterableEquals(before, getHistories());
						assertIterableEquals(after, getHistories(manager));
					} catch (SQLException e) {
						throw new RuntimeException(e);
					}
					throw new RuntimeException();
				});
			} catch (RuntimeException e) {
				// Exceptionを発生させるためのRuntimeException => 無視する
			}
			// ロールバック後にもとに戻っている
			assertIterableEquals(before, getHistories());
			assertIterableEquals(before, getHistories(manager));
		}

		// リトライ可能な例外でリトライされるケース
		try (PhoneBillDbManager manager = PhoneBillDbManager.createPhoneBillDbManager(config)) {
			List<History> before = getHistories();
			List<History> after = new ArrayList<>(before);

			// 1レコード更新
			final History h = before.get(0).clone();
			h.setCharge(h.getCharge() == null ? 300 : h.getCharge() + 300);
			after.set(0, h);
			HistoryDao dao = new HistoryDaoJdbc((PhoneBillDbManagerJdbc) manager) {

				@Override
				public int update(History history) {
					if (++tryCount < 4) {
						throw new RuntimeException(new SerializationFailureException("40001"));
					}
					return super.update(history);
				}
			};

			tryCount = 0;
			manager.execute(TxOption.of(10), () -> dao.update(h));
			// コミット後に別コネクションでも更新が反映されている
			assertIterableEquals(after, getHistories());
			assertIterableEquals(after, getHistories(manager));
			assertEquals(4, tryCount);
		}

		// ロールバックに失敗するケース
		RuntimeException testException = new RuntimeException();
		try (PhoneBillDbManagerJdbc manager = new DbManagerRollbackFailure(config, testException)) {
			tryCount = 0;
			RuntimeException e = assertThrows(RuntimeException.class,
					() -> manager.execute(TxOption.of(), () -> {
						if (++tryCount <= 1) {
							throw new RuntimeException(new SerializationFailureException("40001"));
						}
						assert false; // ロールバックに失敗した場合はリトライされないのでここに制御がこないはず。
						return this; // public <T> T execute()が呼ばれるように何らかの値を返す。
					}));
			assertEquals(SerializationFailureException.class, e.getCause().getClass());
			assertEquals(testException, e.getSuppressed()[0]);
		}
	}


	@Test
	final void testIsRetriableThrowable() throws IOException {
		// 引数にSQLExceptionが渡されたとき
		assertTrue(getManagerOracle().isRetriableSQLException(new ORA8177()));
		assertFalse(getManagerOracle().isRetriableSQLException(new SQLException()));

		// 引数にSQLExceptionをラッピングしたRuntimeExceptionが渡されたとき
		assertTrue(getManagerOracle().isRetriable(new RuntimeException(new ORA8177())));
		assertFalse(getManagerOracle().isRetriable(new RuntimeException(new SQLException())));

		// 複数回ネストしたExceptionが渡されたとき
		assertTrue(getManagerOracle().isRetriable(new RuntimeException(new RuntimeException(new ORA8177()))));
		assertFalse(getManagerOracle().isRetriable(new RuntimeException(new RuntimeException(new SQLException()))));

		// SQLException以外のExceptionが渡されとき
		assertFalse(getManagerOracle().isRetriable(new Exception()));
		assertFalse(getManagerOracle().isRetriable(new RuntimeException(new Exception())));
		assertFalse(getManagerOracle().isRetriable(new RuntimeException(new RuntimeException(new Exception()))));
	}

	@Test
	final void testGetConnection() throws Exception {
		// Configの指定に従ったDBMSのコネクションが取得できること

		Connection conn = getManagerPostgresql().getConnection();
		assertTrue(conn.getClass().getName().toLowerCase(Locale.ROOT).contains("postgresql"));

		// 指定のIsorationLevelが設定されていること
		Config config = getConfigPostgresql().clone();
		config.isolationLevel = IsolationLevel.READ_COMMITTED;
		try (PhoneBillDbManagerJdbc manager = (PhoneBillDbManagerJdbc) PhoneBillDbManager
				.createPhoneBillDbManager(config)) {
			assertEquals(Connection.TRANSACTION_READ_COMMITTED, manager.getConnection().getTransactionIsolation());
		}
		config.isolationLevel = IsolationLevel.SERIALIZABLE;
		try (PhoneBillDbManagerJdbc manager = (PhoneBillDbManagerJdbc) PhoneBillDbManager
				.createPhoneBillDbManager(config)) {
			assertEquals(Connection.TRANSACTION_SERIALIZABLE, manager.getConnection().getTransactionIsolation());
		}

		// コネクションの取得に失敗するケース
		config.url = "bad url";
		try (PhoneBillDbManagerJdbc manager = (PhoneBillDbManagerJdbc) PhoneBillDbManager
				.createPhoneBillDbManager(config, SessionHoldingType.INSTANCE_FIELD)) {
			RuntimeException e = assertThrows(RuntimeException.class, () -> manager.getConnection().getTransactionIsolation());
			assertEquals(SQLException.class, e.getCause().getClass());
		}
		try (PhoneBillDbManagerJdbc manager = (PhoneBillDbManagerJdbc) PhoneBillDbManager
				.createPhoneBillDbManager(config, SessionHoldingType.THREAD_LOCAL)) {
			RuntimeException e = assertThrows(RuntimeException.class, () -> manager.getConnection().getTransactionIsolation());
			assertEquals(SQLException.class, e.getCause().getClass());
		}
	}

	@Test
	@Tag("oracle")
	final void testGetConnectionOracle() throws Exception {
		// Configの指定に従ったDBMSのコネクションが取得できること
		Connection conn = getManagerOracle().getConnection();
		assertTrue(conn.isValid(1));
		assertTrue(conn.getClass().getName().toLowerCase(Locale.ROOT).contains("oracle"));

		// 指定のIsorationLevelが設定されていること
		Config config = getConfigOracle().clone();
		config.isolationLevel = IsolationLevel.READ_COMMITTED;
		try (PhoneBillDbManagerJdbc manager = (PhoneBillDbManagerJdbc) PhoneBillDbManager
				.createPhoneBillDbManager(config)) {
			assertEquals(Connection.TRANSACTION_READ_COMMITTED, manager.getConnection().getTransactionIsolation());
		}
		config.isolationLevel = IsolationLevel.SERIALIZABLE;
		try (PhoneBillDbManagerJdbc manager = (PhoneBillDbManagerJdbc) PhoneBillDbManager
				.createPhoneBillDbManager(config)) {
			assertEquals(Connection.TRANSACTION_SERIALIZABLE, manager.getConnection().getTransactionIsolation());
		}

		// コネクションの取得に失敗するケース
		config.url = "bad url";
		try (PhoneBillDbManagerJdbc manager = (PhoneBillDbManagerJdbc) PhoneBillDbManager
				.createPhoneBillDbManager(config, SessionHoldingType.INSTANCE_FIELD)) {
			RuntimeException e = assertThrows(RuntimeException.class, () -> manager.getConnection().getTransactionIsolation());
			assertEquals(SQLException.class, e.getCause().getClass());
		}
		try (PhoneBillDbManagerJdbc manager = (PhoneBillDbManagerJdbc) PhoneBillDbManager
				.createPhoneBillDbManager(config, SessionHoldingType.THREAD_LOCAL)) {
			RuntimeException e = assertThrows(RuntimeException.class, () -> manager.getConnection().getTransactionIsolation());
			assertEquals(SQLException.class, e.getCause().getClass());
		}

	}


	@Test
	final void testIsRetriableSQLException() throws IOException {
		// Ora-8177のケース
		assertTrue(getManagerOracle().isRetriableSQLException(new ORA8177()));
		assertFalse(getManagerPostgresql().isRetriableSQLException(new ORA8177()));

		// PostgreSQLでserialization_failureが起きたときのケース
		assertFalse(getManagerOracle().isRetriableSQLException(new SerializationFailureException("40001")));
		assertTrue(getManagerPostgresql().isRetriableSQLException(new SerializationFailureException("40001")));

		// PostgreSQLでserialization_failure以外のExceptionが起きたときのケース
		assertFalse(getManagerOracle().isRetriableSQLException(new SerializationFailureException("20001")));
		assertFalse(getManagerPostgresql().isRetriableSQLException(new SerializationFailureException("20001")));

		// 上記のいずれでもないSQLExceptionのケース
		assertFalse(getManagerOracle().isRetriableSQLException(new SQLException()));
		assertFalse(getManagerPostgresql().isRetriableSQLException(new SQLException()));
	}

	private static class ORA8177 extends SQLException {
		@Override
		public int getErrorCode() {
			return 8177;
		}
	}

	private static class SerializationFailureException extends PSQLException {
		String errorCode;

		public SerializationFailureException(String errorCode) {
			super("serialization_failure", null);
			this.errorCode = errorCode;
		}

		@Override
		public String getSQLState() {
			return errorCode;
		}
	}

	@Test
	final void testSessionHoldingType() throws IOException {
		Config config = Config.getConfig();
		assertEquals(5, createThreadAndCountConnections(config, 5, SessionHoldingType.THREAD_LOCAL));
		assertEquals(1, createThreadAndCountConnections(config, 5, SessionHoldingType.INSTANCE_FIELD));
	}

	/**
	 * 指定した数のスレッドで、PhoneBillDbManagerJdbcのインスタンスを共有し、各スレッドげ
	 * コネクションを取得したときに、取得できるコネクション数を取得する。併せて、PhoneBillDbManagerJdbc#close()の
	 * 呼び出しで、すべてのJDBCコネクションがクローズされることを確認する。
	 *
	 * @param config
	 * @param threads
	 * @param type
	 * @return コネクション数: 各スレッドでコネクションを共有している場合は1
	 */
	private int createThreadAndCountConnections(Config config, int threads, SessionHoldingType type) {
		int connectionCount;
		List<Future<?>> futures = new ArrayList<>();
		try (PhoneBillDbManagerJdbc manager = (PhoneBillDbManagerJdbc) PhoneBillDbManagerJdbc
				.createPhoneBillDbManager(config, type)) {
			ExecutorService service = Executors.newFixedThreadPool(threads);
			List<Task> tasks = new ArrayList<>();
			Set<Connection> connections = new HashSet<>();
			;
			for (int i = 0; i < threads; i++) {
				Task task = new Task(manager);
				tasks.add(task);
				Future<?> f = service.submit(task);
				futures.add(f);
				task.waitForConnectionGet();
				connections.add(task.connection);
			}

			// すべてのコネクションがアクティブなこと
			connectionCount = connections.size();
			connections.stream().forEach(c -> {
				try {
					assertTrue(c.isValid(1));
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
			});

			// manager.close()によりすべてのコネクションがクローズされること
			manager.close();
			connections.stream().forEach(c -> {
				try {
					assertTrue(c.isClosed());
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
			});

			tasks.stream().forEach(t -> t.end.countDown());
			futures.stream().forEach(f -> {
				try {
					f.get();
				} catch (InterruptedException | ExecutionException e) {
					throw new RuntimeException(e);
				}
			});
			service.shutdown();
		}
		return connectionCount;
	}


	private static class Task implements Runnable {
		final PhoneBillDbManagerJdbc manager;
		final CountDownLatch connectionGotten = new CountDownLatch(1);
		final CountDownLatch end = new CountDownLatch(1);
		Connection connection;

		public Task(PhoneBillDbManagerJdbc manager) {
			this.manager = manager;
		}

		@Override
		public void run() {
			connection = manager.getConnection();
			connectionGotten.countDown();
			for (;;) {
				try {
					end.await();
					return;
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		}

		public void waitForConnectionGet() {
			for (;;) {
				try {
					connectionGotten.await();
					return;
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}

	/**
	 * クローズ時にSQLExceptionがスローされるケース
	 *
	 * @throws Exception
	 */
	@Test
	void testCloseThoresSQLException() throws Exception {
		Config config = Config.getConfig();
		try (PhoneBillDbManagerJdbc manager = (PhoneBillDbManagerJdbc) PhoneBillDbManager
				.createPhoneBillDbManager(config, SessionHoldingType.THREAD_LOCAL)) {
			// リフレクションを使用してコネクションのリストを取得
			Field field = PhoneBillDbManagerJdbc.class.getDeclaredField("connectionList");
			field.setAccessible(true);
			@SuppressWarnings("unchecked")
			List<Connection> list = (List<Connection>) field.get(manager);
			for(Connection conn: list) {
				conn.close();
			}
			// クローズ時にSQLExceptionをスローするテスト用のコネクションをセット
			list.clear();
			SQLException sqlException = new SQLException();
			list.add(new ConnectionForTestClose(sqlException));
			list.add(new ConnectionForTestClose(sqlException));
			list.add(new ConnectionForTestClose(sqlException));

			// 指定のSQLExceptionをラップ下RuntimeExceptionがスローされることを確認
			RuntimeException e = assertThrows(RuntimeException.class, () -> manager.close());
			assertEquals(sqlException, e.getCause());

			// すべてのコネクションのcloseが呼ばれていることを確認
			for (Connection conn : list) {
				ConnectionForTestClose c = (ConnectionForTestClose) conn;
				assertTrue(c.closeCalleed);
			}
			list.clear();
		}
	}

	/**
	 * java.sql.Connectionのclose時にSQLExceptionが発生したときのテストのためのテストクラス
	 */
	private static class ConnectionForTestClose implements Connection {
		SQLException sqlException;
		private boolean closeCalleed = false;

		public ConnectionForTestClose(SQLException sqlException) {
			this.sqlException = sqlException;
		}

		@Override
		public <T> T unwrap(Class<T> iface) throws SQLException {
			assert false;
			return null;
		}

		@Override
		public boolean isWrapperFor(Class<?> iface) throws SQLException {
			assert false;
			return false;
		}

		@Override
		public Statement createStatement() throws SQLException {
			assert false;
			return null;
		}

		@Override
		public PreparedStatement prepareStatement(String sql) throws SQLException {
			assert false;
			return null;
		}

		@Override
		public CallableStatement prepareCall(String sql) throws SQLException {
			assert false;
			return null;
		}

		@Override
		public String nativeSQL(String sql) throws SQLException {
			assert false;
			return null;
		}

		@Override
		public void setAutoCommit(boolean autoCommit) throws SQLException {
			assert false;
		}

		@Override
		public boolean getAutoCommit() throws SQLException {
			assert false;
			return false;
		}

		@Override
		public void commit() throws SQLException {
			assert false;
		}

		@Override
		public void rollback() throws SQLException {
			assert false;
		}

		@Override
		public void close() throws SQLException {
			closeCalleed = true;
			throw sqlException;
		}

		@Override
		public boolean isClosed() throws SQLException {
			assert false;
			return false;
		}

		@Override
		public DatabaseMetaData getMetaData() throws SQLException {
			assert false;
			return null;
		}

		@Override
		public void setReadOnly(boolean readOnly) throws SQLException {
			assert false;
		}

		@Override
		public boolean isReadOnly() throws SQLException {
			assert false;
			return false;
		}

		@Override
		public void setCatalog(String catalog) throws SQLException {
			assert false;
		}

		@Override
		public String getCatalog() throws SQLException {
			assert false;
			return null;
		}

		@Override
		public void setTransactionIsolation(int level) throws SQLException {
			assert false;
		}

		@Override
		public int getTransactionIsolation() throws SQLException {
			assert false;
			return 0;
		}

		@Override
		public SQLWarning getWarnings() throws SQLException {
			assert false;
			return null;
		}

		@Override
		public void clearWarnings() throws SQLException {
			assert false;
		}

		@Override
		public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
			assert false;
			return null;
		}

		@Override
		public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
				throws SQLException {
			assert false;
			return null;
		}

		@Override
		public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
				throws SQLException {
			assert false;
			return null;
		}

		@Override
		public Map<String, Class<?>> getTypeMap() throws SQLException {
			assert false;
			return null;
		}

		@Override
		public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
			assert false;
		}

		@Override
		public void setHoldability(int holdability) throws SQLException {
			assert false;
		}

		@Override
		public int getHoldability() throws SQLException {
			assert false;
			return 0;
		}

		@Override
		public Savepoint setSavepoint() throws SQLException {
			assert false;
			return null;
		}

		@Override
		public Savepoint setSavepoint(String name) throws SQLException {
			assert false;
			return null;
		}

		@Override
		public void rollback(Savepoint savepoint) throws SQLException {
			assert false;
		}

		@Override
		public void releaseSavepoint(Savepoint savepoint) throws SQLException {
			assert false;
		}

		@Override
		public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
				throws SQLException {
			assert false;
			return null;
		}

		@Override
		public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
				int resultSetHoldability) throws SQLException {
			assert false;
			return null;
		}

		@Override
		public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
				int resultSetHoldability) throws SQLException {
			assert false;
			return null;
		}

		@Override
		public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
			assert false;
			return null;
		}

		@Override
		public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
			assert false;
			return null;
		}

		@Override
		public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
			assert false;
			return null;
		}

		@Override
		public Clob createClob() throws SQLException {
			assert false;
			return null;
		}

		@Override
		public Blob createBlob() throws SQLException {
			assert false;
			return null;
		}

		@Override
		public NClob createNClob() throws SQLException {
			assert false;
			return null;
		}

		@Override
		public SQLXML createSQLXML() throws SQLException {
			assert false;
			return null;
		}

		@Override
		public boolean isValid(int timeout) throws SQLException {
			assert false;
			return false;
		}

		@Override
		public void setClientInfo(String name, String value) throws SQLClientInfoException {
			assert false;
		}

		@Override
		public void setClientInfo(Properties properties) throws SQLClientInfoException {
			assert false;
		}

		@Override
		public String getClientInfo(String name) throws SQLException {
			assert false;
			return null;
		}

		@Override
		public Properties getClientInfo() throws SQLException {
			assert false;
			return null;
		}

		@Override
		public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
			assert false;
			return null;
		}

		@Override
		public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
			assert false;
			return null;
		}

		@Override
		public void setSchema(String schema) throws SQLException {
			assert false;
		}

		@Override
		public String getSchema() throws SQLException {
			assert false;
			return null;
		}

		@Override
		public void abort(Executor executor) throws SQLException {
			assert false;
		}

		@Override
		public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
			assert false;
		}

		@Override
		public int getNetworkTimeout() throws SQLException {
			assert false;
			return 0;
		}
	}

	/**
	 * ロールバックに失敗するケースのテストのためのテストクラス.
	 * ロールバック時にコンストラクタで指定したExceptionをスローする。
	 */
	private static class DbManagerRollbackFailure extends PhoneBillDbManagerPostgresql {
		RuntimeException e;

		public DbManagerRollbackFailure(Config config, RuntimeException e) {
			super(config, SessionHoldingType.INSTANCE_FIELD);
			this.e = e;
		}

		@Override
		public void rollback(Consumer<TsurugiTransaction> listener) {
			throw e;
		}
	}
}
