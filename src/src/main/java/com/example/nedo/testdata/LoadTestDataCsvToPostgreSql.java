package com.example.nedo.testdata;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.app.Config;
import com.example.nedo.app.Config.Dbms;
import com.example.nedo.db.jdbc.DBUtils;
import com.example.nedo.app.CreateTable;
import com.example.nedo.app.ExecutableCommand;
import com.example.nedo.util.PathUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class LoadTestDataCsvToPostgreSql extends ExecutableCommand {
    private static final Logger LOG = LoggerFactory.getLogger(LoadTestDataCsvToPostgreSql.class);

    public static void main(String[] args) throws Exception {
		Config config = Config.getConfig(args);
		LoadTestDataCsvToPostgreSql loadTestDataCsvToPostgreSql = new LoadTestDataCsvToPostgreSql();
		loadTestDataCsvToPostgreSql.execute(config);
	}

	@Override
	public void execute(Config config) throws Exception {
		if (config.dbms != Dbms.POSTGRE_SQL) {
			LOG.error("This configuration is not for the PostgreSQL.");
		} else {
			try (Connection conn = DBUtils.getConnection(config);
					Statement stmt = conn.createStatement()) {
				conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
				conn.setAutoCommit(false);
				CreateTable.prepareLoadData(stmt, config);
				Path dir = Paths.get(config.csvDir);
				List<Path> contractsList = Collections.singletonList(CsvUtils.getContractsFilePath(dir));
				List<Path> historyList = CsvUtils.getHistortyFilePaths(dir);
				doCopy(stmt, config, "contracts", contractsList);
				doCopy(stmt, config, "history", historyList);
				CreateTable.afterLoadData(stmt, config);
			}
		}
	}

	/**
	 * 指定のテーブルにCSVファイルをロードする
	 *
	 * @param conn
	 * @param tablename
	 * @throws SQLException
	 */
	@SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
	private void doCopy(Statement stmt, Config config, String tablename, List<Path> pathList) throws SQLException {
		String sql;
		sql = "truncate table " + tablename;
		stmt.execute(sql); // truncateとcopyを同一トランザックションにするこで高速化が期待できる

		long startTime = System.currentTimeMillis();

		for (Path path : pathList) {
			String pathStr = PathUtils.toWls(path.toAbsolutePath(), File.separatorChar == '\\');
			sql = "copy " + tablename + " from '" + pathStr + "' with csv";
			LOG.info("start sql: " + sql);
			stmt.execute(sql);
		}

		stmt.getConnection().commit();
		long elapsedTime = System.currentTimeMillis() - startTime;
		String format = "end sql: " + sql + " in %,.3f sec ";
		LOG.info(String.format(format, elapsedTime / 1000d));
	}


}
