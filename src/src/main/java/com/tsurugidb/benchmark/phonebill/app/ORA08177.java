package com.tsurugidb.benchmark.phonebill.app;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.phonebill.db.jdbc.PhoneBillDbManagerJdbc;

/**
 * ORA-08177を再現するためのTP
 *
 */
public class ORA08177 extends ExecutableCommand {
    private static final Logger LOG = LoggerFactory.getLogger(ORA08177.class);


	private Statement stmt;

	public static void main(String[] args) throws Exception {
		Config config = Config.getConfig(false);
		new CreateTable().execute(config);
		new ORA08177().execute(config);
	}

	@Override
	public void execute(Config config) throws Exception {
		// TODO: DAOを使用するように変更する
		PhoneBillDbManagerJdbc manager = config.getDbManagerJdbc();

		Connection conn = manager.getConnection();;
		conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
		stmt = conn.createStatement();

		String create_table = "create table test(c integer, primary key(c)) SEGMENT CREATION DEFERRED ";
		execSQL("drop table test");
		execSQL(create_table);
		execSQL("truncate table test");

		for (int i = 0; i < 10000; i++) {
			if (i == 579) {
				LOG.info("NOW");
			}
			String sql = "insert into test(c) values(" + i + ")";
			execSQL(sql);
			conn.commit();
		}
	}


	private void  execSQL(String sql) throws SQLException {
		LOG.info("Executing sql: {}", sql);
		try {
			stmt.execute(sql);
		} catch (SQLException e) {
			if (e.getErrorCode() != 942) {
				throw e;
			}
		}
	}
}
