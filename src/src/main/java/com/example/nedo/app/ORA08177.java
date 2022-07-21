package com.example.nedo.app;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.db.jdbc.DBUtils;

/**
 * ORA-08177を再現するためのクラス
 *
 */
public class ORA08177 extends ExecutableCommand {
    private static final Logger LOG = LoggerFactory.getLogger(ORA08177.class);


	private Statement stmt;

	public static void main(String[] args) throws Exception {
		Config config = Config.getConfig(args);
		new CreateTable().execute(config);
		new ORA08177().execute(config);
	}

	@Override
	public void execute(Config config) throws Exception {
		Connection conn = DBUtils.getConnection(config);
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
