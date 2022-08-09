package com.tsurugidb.benchmark.phonebill.db.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.doma2.dao.ContractDao;
import com.tsurugidb.benchmark.phonebill.db.doma2.dao.HistoryDao;
import com.tsurugidb.iceaxe.transaction.TgTmSetting;

public abstract class PhoneBillDbManagerJdbc extends PhoneBillDbManager {
    private static final Logger LOG = LoggerFactory.getLogger(PhoneBillDbManagerJdbc.class);

	private final List<Connection> connectionList = new CopyOnWriteArrayList<>();

	private final ThreadLocal<Connection> connectionThreadLocal = new ThreadLocal<Connection>() {
		@Override
		protected Connection initialValue() {
			try {
				Connection c = createConnection();
				connectionList.add(c);
				return c;
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
	};

	/**
	 * JDBCコネクションを作成する
	 *
	 * @return
	 * @throws SQLException
	 */
	protected abstract Connection createConnection() throws SQLException;


	/**
	 * 指定のSQLExceptionを調べ、リトライにより回復可能な場合はtrueを返す
	 *
	 * @param e
	 * @return
	 */
	protected abstract boolean isRetriable(SQLException e);

	/**
	 * スレッドローカルなコネクションを取得する。
	 *
	 * @return
	 */
	public Connection getConnection() {
		return connectionThreadLocal.get();
	}

	@Override
	public void commit(Runnable listener) {
		Connection c = getConnection();
		try {
			c.commit();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		if (listener != null) {
			listener.run();
		}
	}

	@Override
	public void rollback(Runnable listener) {
		Connection c = getConnection();
		try {
			c.rollback();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		if (listener != null) {
			listener.run();
		}
	}

	@Override
	public void close() {
		RuntimeException exception = null;

		for (Connection c : connectionList) {
			try {
				c.close();
			} catch (SQLException e) {
				if (exception == null) {
					exception = new RuntimeException(e);
				} else {
					exception.addSuppressed(e);
				}
			}
		}

		if (exception != null) {
			throw exception;
		}
	}

	@Override
	public void execute(TgTmSetting setting, Runnable runnable) {
		for (;;) {
			try {
				runnable.run();
				commit();
				break;
			} catch (Throwable e) {
				try {
					rollback();
					if (e.getCause() instanceof SQLException) {
						SQLException se = (SQLException) e.getCause();
						if (isRetriable(se)) {
							LOG.debug("{} caught {} retriable exception, ErrorCode = {}, SQLStatus = {}.",
									this.getClass().getName(), se.getErrorCode(), se.getSQLState(), se);
							continue;
						}
					}
				} catch (Throwable t) {
					e.addSuppressed(t);
				}
				throw e;
			}
		}
	}

	@Override
	public <T> T execute(TgTmSetting setting, Supplier<T> supplier) {
		for (;;) {
			try {
				T r = supplier.get();
				commit();
				return r;
			} catch (Throwable e) {
				try {
					rollback();
					if (e.getCause() instanceof SQLException) {
						SQLException se = (SQLException) e.getCause();
						if (isRetriable(se)) {
							LOG.debug("{} caught {} retriable exception, ErrorCode = {}, SQLStatus = {}.",
									this.getClass().getName(), se.getErrorCode(), se.getSQLState(), se);
							continue;
						}
					}
				} catch (Throwable t) {
					e.addSuppressed(t);
				}
				throw e;
			}
		}
	}

	private ContractDao contractDao;

	@Override
	public synchronized ContractDao getContractDao() {
		if (contractDao == null) {
			contractDao = new ContractDaoJdbc(this);

		}
		return contractDao;
	}

	private HistoryDao historyDao;

	@Override
	public synchronized HistoryDao getHistoryDao() {
		if (historyDao == null) {
			historyDao = new HistoryDaoJdbc(this);
		}
		return historyDao;
	}


	/**
	 * {@link PhoneBillDbManagerJdbc#getConnection()}で取得できるコネクションと別の
	 * コネクションを取得する。UTで同一のスレッドで別のコネクションが必要になったときに使用する。
	 * UT以外での使用禁止。
	 *
	 * @return
	 * @throws SQLException
	 */
	public Connection getIsoratedConnection() throws SQLException {
		return createConnection();
	}
}
