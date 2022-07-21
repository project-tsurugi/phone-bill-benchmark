package com.example.nedo.db.jdbc;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.example.nedo.app.Config;
import com.example.nedo.app.Config.DbmsType;
import com.example.nedo.db.SessionException;

import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

/**
 * JDBCのセッションを保持するクラス.
 * <br>
 * 内部に{@link java.sql.Connection}のフィルードを保持し、これにアクセスするためのメソッドを提供する。
 *
 *  */
public class Session implements Closeable {
	private Connection conn;

	public static Session getSession(Config config)  {
		return new Session(config);
	}

	private Session(Config config) {
		try {
			if (config.dbmsType == DbmsType.ORACLE_JDBC) {
				PoolDataSource pds = PoolDataSourceFactory.getPoolDataSource();
				pds.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
				pds.setURL(config.url);
				pds.setUser(config.user);
				pds.setPassword(config.password);
				pds.setMaxStatements(256);
				conn = pds.getConnection();
			} else {
				conn = DriverManager.getConnection(config.url, config.user, config.password);
			}
			conn.setAutoCommit(false);
			switch(config.isolationLevel) {
			case READ_COMMITTED:
				conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
				break;
			case SERIALIZABLE:
				conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
				break;
			default:
				assert false;
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public Connection getConnection() {
		return conn;
	}

	@Override
	public void close() throws IOException {
		try {
			if (conn.isClosed()) {
				conn.close();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public void commit() throws SessionException {
		try {
			conn.commit();
		} catch (SQLException e) {
			throw new SessionException(e);
		}
	}
}
