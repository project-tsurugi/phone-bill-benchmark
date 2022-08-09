package com.tsurugidb.benchmark.phonebill.db.postgresql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.interfaces.DdlLExecutor;
import com.tsurugidb.benchmark.phonebill.db.jdbc.PhoneBillDbManagerJdbc;

public class PhoneBillDbManagerPostgresql extends PhoneBillDbManagerJdbc {
	private DdlLExecutor ddlLExecutor;
	private Config config;

	public PhoneBillDbManagerPostgresql(Config config) {
		this.config = config;
	}

	private PhoneBillDbManagerPostgresql(PhoneBillDbManagerPostgresql manager) {
		super(manager);
		this.config = manager.config;
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
	public synchronized DdlLExecutor getDdlLExecutor() {
		if (ddlLExecutor == null) {
			ddlLExecutor = new DdlExectorPostgresql(this);
		}
		return ddlLExecutor;
	}

	@Override
	public boolean isRetriable(SQLException e) {
		if (e.equals("40001")) {
			// シリアライゼーション失敗
			return true;
		}
		return false;
	}

	@Override
	public PhoneBillDbManager creaetSessionSharedInstance() {
		return new PhoneBillDbManagerPostgresql(this);
	}

}
