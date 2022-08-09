package com.tsurugidb.benchmark.phonebill.db.oracle;

import java.sql.Connection;
import java.sql.SQLException;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.PhoneBillDbManager;
import com.tsurugidb.benchmark.phonebill.db.interfaces.DdlLExecutor;
import com.tsurugidb.benchmark.phonebill.db.jdbc.PhoneBillDbManagerJdbc;

import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

public class PhoneBillDbManagerOracle extends PhoneBillDbManagerJdbc {
	private DdlLExecutor ddlLExecutor;
	private Config config;

	public PhoneBillDbManagerOracle(Config config) {
		this.config = config;
	}

	private  PhoneBillDbManagerOracle(PhoneBillDbManagerOracle manager) {
		super(manager);
		this.config = manager.config;
	}

	@Override
	protected Connection createConnection() throws SQLException {
		Connection conn;
		PoolDataSource pds = PoolDataSourceFactory.getPoolDataSource();
		pds.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
		pds.setURL(config.url);
		pds.setUser(config.user);
		pds.setPassword(config.password);
		pds.setMaxStatements(256);
		conn = pds.getConnection();
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
			ddlLExecutor = new DdlLExecutorOracle(this, config);
		}
		return ddlLExecutor;
	}

	@Override
	public boolean isRetriable(SQLException e) {
		// 「ORA-08177: このトランザクションのアクセスをシリアル化できません」発生時true
		if (e.getErrorCode() == 8177) {
			return true;
		}
		return false;
	}

	@Override
	public PhoneBillDbManager creaetSessionSharedInstance() {
		return new PhoneBillDbManagerOracle(this);
	}
}
