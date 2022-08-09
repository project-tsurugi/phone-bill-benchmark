package com.tsurugidb.benchmark.phonebill.db.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.doma2.dao.BillingDao;
import com.tsurugidb.benchmark.phonebill.db.doma2.dao.ContractDao;
import com.tsurugidb.benchmark.phonebill.db.doma2.dao.HistoryDao;
import com.tsurugidb.iceaxe.transaction.TgTmSetting;

public abstract class PhoneBillDbManagerJdbc extends PhoneBillDbManager {
    private static final Logger LOG = LoggerFactory.getLogger(PhoneBillDbManagerJdbc.class);

	private final List<Connection> connectionList = new CopyOnWriteArrayList<>();

	private final ConnectionHolder connectionHolder;


	/**
	 * デフォルトコンストラクタでは、スレッドローカルなコネクションを保持するインスタンスを生成する
	 */
	public PhoneBillDbManagerJdbc() {
		connectionHolder = new ThreadLocalConnectionHolder();
	}

	/**
	 * 指定のPhoneBillDbManagerJdbcとコネクションを共有するインスタンスを作成する
	 */
	protected PhoneBillDbManagerJdbc(PhoneBillDbManagerJdbc phoneBillDbManagerJdbc)  {
		connectionHolder = new SimpleConnectionHolder(phoneBillDbManagerJdbc.getConnection());
	}


	/**
	 * JDBCコネクションを作成する
	 *
	 * @return
	 * @throws SQLException
	 */
	protected abstract Connection createConnection() throws SQLException;


	public Connection getConnection() {
		return connectionHolder.getConnection();
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
					if (isRetriable(e)) {
						continue;
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
					if (isRetriable(e)) {
						continue;
					}
	            } catch (Throwable t) {
	                e.addSuppressed(t);
	            }
	            throw e;
			}
		}
	}


	/**
	 * Throwableがリトライ可能か調べる
	 */
	public boolean isRetriable(Throwable t) {
		while (t != null) {
			if (t instanceof SQLException) {
				SQLException se = (SQLException) t;
				boolean ret = isRetriable(se);
				LOG.debug("{} caught {} retriable exception, ErrorCode = {}, SQLStatus = {}.",
						this.getClass().getName(), se.getErrorCode(), se.getSQLState(), se);

				return ret;
			}
			t = t.getCause();
		}
		return false;
	}


	/**
	 * SQLExceptionがリトライ可能か調べる
	 */
	public abstract boolean isRetriable(SQLException e);


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

	// DAOの取得

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

	private BillingDao billingDao;

	@Override
	public synchronized BillingDao getBillingDao() {
		if (billingDao == null) {
			billingDao = new BillingDaoJdbc(this);
		}
		return billingDao;
	}

	// ConnectionHolder

	private interface ConnectionHolder {
		Connection getConnection();
	}


	private class SimpleConnectionHolder implements ConnectionHolder{
		Connection conn;

		public SimpleConnectionHolder(Connection conn) {
			this.conn = conn;
			connectionList.add(conn);
		}

		@Override
		public Connection getConnection() {
			return conn;
		}
	}

	private class ThreadLocalConnectionHolder implements ConnectionHolder{
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

		@Override
		public Connection getConnection() {
			return connectionThreadLocal.get();
		}
	}
}
