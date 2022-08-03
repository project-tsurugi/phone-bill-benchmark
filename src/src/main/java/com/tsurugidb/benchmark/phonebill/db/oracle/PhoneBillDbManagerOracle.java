package com.tsurugidb.benchmark.phonebill.db.oracle;

import java.sql.Connection;
import java.sql.SQLException;

import com.tsurugidb.benchmark.phonebill.app.Config;
import com.tsurugidb.benchmark.phonebill.db.interfaces.DdlLExecutor;
import com.tsurugidb.benchmark.phonebill.db.jdbc.PhoneBillDbManagerJdbc;

import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

public class PhoneBillDbManagerOracle extends PhoneBillDbManagerJdbc {
	private DdlLExecutor ddlLExecutor;
	private Config config;

	public PhoneBillDbManagerOracle(Config config) {
		super(config);
		this.config = config;
		ddlLExecutor = new DdlLExecutorOracle(this, config);
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
	public DdlLExecutor getDdlLExecutor() {
		return ddlLExecutor;
	}

}
