package com.tsurugidb.benchmark.phonebill.db.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.dao.BillingDao;
import com.tsurugidb.benchmark.phonebill.db.dao.ContractDao;
import com.tsurugidb.benchmark.phonebill.db.dao.HistoryDao;
import com.tsurugidb.benchmark.phonebill.db.jdbc.dao.BillingDaoJdbc;
import com.tsurugidb.benchmark.phonebill.db.jdbc.dao.ContractDaoJdbc;
import com.tsurugidb.benchmark.phonebill.db.jdbc.dao.HistoryDaoJdbc;
import com.tsurugidb.iceaxe.transaction.TgTmSetting;

public abstract class PhoneBillDbManagerJdbc extends PhoneBillDbManager {
    private static final Logger LOG = LoggerFactory.getLogger(PhoneBillDbManagerJdbc.class);

	private final List<Connection> connectionList = new CopyOnWriteArrayList<>();

	private final ConnectionHolder connectionHolder;


	public PhoneBillDbManagerJdbc(SessionHoldingType type) {
		switch(type) {
		case INSTANCE_FIELD:
			connectionHolder = new InstanceFieldHolder();
			break;
		case THREAD_LOCAL:
			connectionHolder = new ThreadLocalConnectionHolder();
			break;
		default:
			connectionHolder = null;
			assert false;
		}
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


	private class InstanceFieldHolder implements ConnectionHolder {
		private volatile Connection c;

		@Override
		public Connection getConnection() {
			if (c == null) {
				synchronized (this) {
					if (c == null) {
						try {
							this.c = createConnection();
							connectionList.add(c);
						} catch (SQLException e) {
							throw new RuntimeException(e);
						}
					}
				}
			}
			return c;
		}
	}

	private class ThreadLocalConnectionHolder implements ConnectionHolder {
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
