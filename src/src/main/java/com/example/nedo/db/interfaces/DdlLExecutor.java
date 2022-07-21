package com.example.nedo.db.interfaces;

import com.example.nedo.app.Config;
import com.example.nedo.db.SessionException;
import com.example.nedo.db.jdbc.Session;
import com.example.nedo.db.oracle.DdlLExecutorOracle;
import com.example.nedo.db.postgresql.DdlExectorPostgresql;

// TODO 引数にconfigをとるメソッドがあるがconfigは不要なはず
public interface DdlLExecutor {
	public static DdlLExecutor getInstance(Config config) {
		switch (config.dbmsType) {
		case ORACLE_JDBC:
			return new DdlLExecutorOracle(config);
		case POSTGRE_SQL_JDBC:
			return new DdlExectorPostgresql();
		default:
			assert false;
			return null;
		}
	}

	public void dropTables(Session session) throws SessionException;

	public void createHistoryTable(Session session) throws SessionException;

	public void createBillingTable(Session session) throws SessionException;

	public void createContractsTable(Session session) throws SessionException;

	/**
	 * インデックスを生成する
	 *
	 * @param session
	 * @param config
	 * @throws SessionException
	 */
	public void createIndexes(Session session) throws SessionException;

	/**
	 * 統計情報を更新する
	 * @throws SessionException
	 */
	public void updateStatistics(Session session) throws SessionException;

	/**
	 * テストデータのロード後の処理.
	 * <br>
	 * プライマリーキー、インデックスを作成し、統計情報を更新する
	 *
	 * @param session
	 * @param config
	 * @throws SessionException
	 */
	public void afterLoadData(Session session) throws SessionException;


	/**
	 * テストデータのロード前の処理.
	 * <br>
	 * プライマリーキー、インデックスとテーブルデータを削除する
	 *
	 * @param session
	 * @param config
	 * @throws SessionException
	 */
	public void prepareLoadData(Session session) throws SessionException;
}
