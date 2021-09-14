package com.example.nedo.testdata;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.app.Config;
import com.example.nedo.app.CreateTable;
import com.example.nedo.app.ExecutableCommand;
import com.example.nedo.db.DBUtils;
import com.example.nedo.util.PathUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class LoadTestDataCsvToPostgreSql implements ExecutableCommand {
    private static final Logger LOG = LoggerFactory.getLogger(LoadTestDataCsvToPostgreSql.class);

    public static void main(String[] args) throws Exception {
		Config config = Config.getConfig(args);
		LoadTestDataCsvToPostgreSql loadTestDataCsvToPostgreSql = new LoadTestDataCsvToPostgreSql();
		loadTestDataCsvToPostgreSql.execute(config);
	}


	@Override
	public void execute(Config config) throws Exception {
		try (Connection conn = DBUtils.getConnection(config);
				Statement stmt = conn.createStatement()) {
			conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
			CreateTable.prepareLoadData(stmt, config);
			doCopy(stmt, config, "contracts");
			doCopy(stmt, config, "history");
			CreateTable.afterLoadData(stmt, config);
		}
	}

	/**
	 * 指定のテーブルにCSVファイルをロードする
	 *
	 * @param conn
	 * @param name
	 * @throws SQLException
	 */
	@SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
	private void doCopy(Statement stmt, Config config, String name) throws SQLException {
		String sql;
		sql = "truncate table " + name;
		stmt.execute(sql); // truncateとcopyを同一トランザックションにするこで高速化が期待できる

		long startTime = System.currentTimeMillis();

		Path csvPath = Paths.get(config.csvDir).resolve(name + ".csv");
		String pathStr = PathUtils.toWls(csvPath.toAbsolutePath());
		sql = "copy " + name + " from '" + pathStr + "' with csv";

		LOG.info("start sql: " + sql);
		stmt.execute(sql);
		stmt.getConnection().commit();
		long elapsedTime = System.currentTimeMillis() - startTime;
		String format = "end sql: " + sql + "in %,.3f sec ";
		LOG.info(String.format(format, elapsedTime / 1000d));
	}


}
