package com.tsurugidb.benchmark.phonebill.db.postgresql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.dao.Ddl;
import com.tsurugidb.benchmark.phonebill.db.jdbc.PhoneBillDbManagerJdbc;
import com.tsurugidb.benchmark.phonebill.db.postgresql.dao.DdlPostgresql;

public class PhoneBillDbManagerPostgresql extends PhoneBillDbManagerJdbc {
	private Ddl ddl;
	private Config config;

	public PhoneBillDbManagerPostgresql(Config config, SessionHoldingType type) {
		super(type);
		this.config = config;
	}

	@Override
	protected Connection createConnection() throws SQLException {
        String url = config.url;
        String user = config.user;
        String password = config.password;
        Connection conn = DriverManager.getConnection(url, user, password);
        conn.setAutoCommit(false);
		switch (config.isolationLevel) {
		case READ_COMMITTED:
			conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
			break;
		case SERIALIZABLE:
			conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			break;
		default:
			assert false;
		}
		return conn;
    }


	@Override
	public synchronized Ddl getDdl() {
		if (ddl == null) {
			ddl = new DdlPostgresql(this);
		}
		return ddl;
	}

	@Override
	public boolean isRetriableSQLException(SQLException e) {
		String sqlState = e.getSQLState();
		if (sqlState != null && sqlState.equals("40001")) {
			// シリアライゼーション失敗
			return true;
		}
		return false;
	}
}
