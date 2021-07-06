package com.example.nedo.testdata;

import java.sql.Connection;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.app.Config;
import com.example.nedo.app.ExecutableCommand;
import com.example.nedo.db.DBUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class CreateTestData implements ExecutableCommand {
    private static final Logger LOG = LoggerFactory.getLogger(CreateTestData.class);

    public static void main(String[] args) throws Exception {
		Config config = Config.getConfig(args);
		CreateTestData createTestData = new CreateTestData();
		createTestData.execute(config);
	}


	@Override
	@SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
	public void execute(Config c) throws Exception {
		// テストデータの作成時は、configの指定にかかわらずTRANSACTION_READ_COMMITTEDを使用する。
		Config config = c.clone();
		config.isolationLevel = Connection.TRANSACTION_READ_COMMITTED;
		TestDataGenerator generator = new TestDataGenerator(config);

		// テーブルをTruncate
		try (Connection conn = DBUtils.getConnection(config);
				Statement stmt = conn.createStatement()) {
			stmt.executeUpdate("truncate table history");
			stmt.executeUpdate("truncate table contracts");
			conn.commit();
		}


		// 契約マスタのテストデータ生成
		long startTime = System.currentTimeMillis();
		generator.generateContracts();
		long elapsedTime = System.currentTimeMillis() - startTime;
		String format = "%,d records generated to contracts table in %,.3f sec ";
		LOG.info(String.format(format, config.numberOfContractsRecords, elapsedTime / 1000d));


		// 通話履歴のテストデータを作成
		startTime = System.currentTimeMillis();
		// TODO 日付をハードコードしているが、configで指定可能にする。
		generator.generateHistory(DBUtils.toDate("2020-11-01"), DBUtils.toDate("2021-01-10"),
				config.numberOfHistoryRecords);
		elapsedTime = System.currentTimeMillis() - startTime;
		format = "%,d records generated to history table in %,.3f sec ";
		LOG.info(String.format(format, config.numberOfHistoryRecords, elapsedTime / 1000d));

		// DBMSの統計情報を更新する
		startTime = System.currentTimeMillis();
		try (Connection conn = DBUtils.getConnection(config);
				Statement stmt = conn.createStatement()) {
			switch (config.dbms) {
			case ORACLE:
				stmt.executeUpdate("{call DBMS_STATS.GATHER_SCHEMA_STATS(ownname => '" + config.user
						+ "', cascade => TRUE, no_invalidate => TRUE)}");
				break;
			case POSTGRE_SQL:
				stmt.executeUpdate("analyze history");
				stmt.executeUpdate("analyze contracts");
				break;
			case OTHER:
				// なにもしない
				break;
			default:
				throw new AssertionError();
			}
			conn.commit();
		}
		elapsedTime = System.currentTimeMillis() - startTime;
		format = "Update statistic in %,.3f sec ";
		LOG.info(String.format(format, elapsedTime / 1000d));

		String report = generator.getStatistics().getReport();
		LOG.info(report);
	}
}
